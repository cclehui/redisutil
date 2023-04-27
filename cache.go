package redisutil

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/cclehui/redisutil/internal/base"
	"github.com/cclehui/redisutil/internal/singleflight"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/gomodule/redigo/redis"
)

type RedisUtil struct {
	pool *redis.Pool

	singleFlightGroupNum int

	logger Logger
}

func NewRedisUtil(pool *redis.Pool, options ...Option) *RedisUtil {
	result := &RedisUtil{pool: pool}

	for _, option := range options {
		option.Apply(result)
	}

	return result
}

func (ru *RedisUtil) SetCache(ctx context.Context, key string, value interface{}, ttl int) (err error) {
	// 判断是否整数
	var bytesData []byte

	if s, ok := isNum(value); ok {
		bytesData = []byte(s)
	} else {
		bytesData, err = base.Encode(value)

		if err != nil {
			return err
		}
	}

	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		if ttl == TTLNoExpire { // 不过期
			_, err = conDo(ctx, con, "SET", keyPatch(key), bytesData)
		} else {
			_, err = conDo(ctx, con, "SET", keyPatch(key), bytesData, "EX", ttl)
		}

		if err != nil {
			return errors.WithStack(err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

type BatchSetParams struct {
	Keys               []string
	Values             []interface{}
	ExpireSecondsSlice []int
}

func (ru *RedisUtil) BatchSet(ctx context.Context, params *BatchSetParams) (err error) {
	defer func() {
		if err != nil {
			ru.getLogger().Errorf(ctx, "CacheUtil.BatchSet, error:%+v", err)
		}
	}()

	if len(params.Keys) != len(params.Values) ||
		len(params.Keys) != len(params.ExpireSecondsSlice) {
		return errors.New("Keys Values ExpireSecondsSlice length is not equal")
	}

	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		for i, key := range params.Keys {
			value := params.Values[i]
			expireSeconds := params.ExpireSecondsSlice[i]

			bytesData, err2 := base.Encode(value)
			if err2 != nil {
				return err2
			}

			if expireSeconds == TTLNoExpire { // 不过期
				err = conSend(ctx, con, "SET", keyPatch(key), bytesData)
			} else {
				err = conSend(ctx, con, "SET", keyPatch(key), bytesData, "EX", expireSeconds)
			}

			if err != nil {
				return errors.WithStack(err)
			}
		}

		err = conFlush(ctx, con)
		if err != nil {
			return errors.WithStack(err)
		}

		for i := 0; i < len(params.Keys); i++ {
			if _, err = conReceive(ctx, con); err != nil {
				return errors.WithStack(err)
			}
		}

		return nil
	})

	return nil
}

func (ru *RedisUtil) GetCache(ctx context.Context, key string, value interface{}) (hit bool, err error) {
	defer func() {
		if err != nil {
			ru.getLogger().Errorf(ctx, "CacheUtil.GetCache, error:%+v", errors.WithStack(err))
		}
	}()

	if reflect.ValueOf(value).Kind() != reflect.Ptr {
		return false, errors.New("value must be ptr")
	}

	var replay []byte

	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		replay, err = redis.Bytes(conDo(ctx, con, "GET", keyPatch(key)))

		if err != nil {
			return err
		}

		return nil
	})

	if err == redis.ErrNil {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	if isNumPtr(value) { // 数字
		err = bytesToNum(replay, value)
	} else {
		err = base.Decode(replay, value)
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func (ru *RedisUtil) MGet(ctx context.Context,
	keys []string, valuesInter interface{}) (hits []bool, err error) {
	defer func() {
		if err != nil {
			ru.getLogger().Errorf(ctx, "CacheUtil.MGet, error:%+v", err)
		}
	}()

	valuesInterRF := reflect.ValueOf(valuesInter)

	if valuesInterRF.Kind() != reflect.Ptr {
		return nil, errors.New(fmt.Sprintf("valuesInter is not ptr: %+v", valuesInter))
	}

	valuesInterRFElem := valuesInterRF.Elem()
	if valuesInterRFElem.Kind() != reflect.Slice {
		return nil, errors.New("valuesInterRFElem[%d] is not slice")
	}

	if len(keys) != valuesInterRFElem.Len() {
		return nil, errors.New("keys and values length must equal")
	}

	var redisResult [][]byte

	// mget 没有命中key的情况下err 也是nil
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		redisResult, err = redis.ByteSlices(conDo(ctx, con, "MGET", keysPatch(keys)...))

		if err != nil {
			return errors.WithStack(err)
		}

		return nil
	})

	hitResult := make([]bool, len(keys))

	if err != nil {
		return nil, err
	}

	for i, dbBytes := range redisResult {
		if dbBytes == nil {
			hitResult[i] = false
			continue
		}

		err = base.DecodeReflectValue(dbBytes, valuesInterRFElem.Index(i))
		if err != nil {
			return nil, err
		}

		hitResult[i] = true
	}

	return hitResult, nil
}

type WrapperParams struct {
	Key           string
	ExpireSeconds int
	Result        interface{} // 结果
	SetFunc       CacheSetFunc

	SingleFlight bool // 是否启动 singleflight
	FlushCache   bool // 是否用SetFunc刷新缓存
}

func (ru *RedisUtil) CacheWrapper(ctx context.Context,
	params *WrapperParams) (err error) {
	if !params.FlushCache {
		if hit, _ := ru.GetCache(ctx, params.Key, params.Result); hit {
			return nil
		}
	} else if reflect.ValueOf(params.Result).Kind() != reflect.Ptr {
		return errors.New("Result must be ptr")
	}

	var newData interface{}

	if params.SingleFlight {
		newData, err, _ = ru.singleflightGroup(params.Key).Do(params.Key, func() (interface{}, error) {
			return ru.cWrapperCallAndSetCache(ctx, params)
		})
	} else {
		newData, err = ru.cWrapperCallAndSetCache(ctx, params)
	}

	if err != nil {
		return err
	}

	resultValue := reflect.ValueOf(params.Result).Elem()
	resultValue.Set(reflect.ValueOf(newData))

	return err
}

func (ru *RedisUtil) cWrapperCallAndSetCache(ctx context.Context,
	params *WrapperParams) (interface{}, error) {
	data, err := params.SetFunc()
	if err != nil {
		return nil, err
	}

	_ = ru.SetCache(ctx, params.Key, data, params.ExpireSeconds)

	return data, nil
}

type WrapperParamsMget struct {
	Keys          []string
	ExpireSeconds []int
	ResultSlice   interface{} // 结果 slice of your result

	SetFuncSlice []MgetSetFunc    // 并发获取
	BatchSetFunc MgetBatchSetFunc // 批量获取

	SingleFlight bool // 是否启动 singleflight
	FlushCache   bool // 用SetFunc刷新缓存
}

// 多key 缓存获取wrapper mget
func (ru *RedisUtil) CacheWrapperMget(ctx context.Context,
	params *WrapperParamsMget) (err error) {
	if len(params.Keys) != len(params.ExpireSeconds) {
		return errors.New("Keys, ExpireSeconds length should equal")
	}

	if len(params.SetFuncSlice) > 0 &&
		len(params.Keys) != len(params.SetFuncSlice) {
		return errors.New("Keys, SetFuncSlice length should equal")
	}

	hits := make([]bool, len(params.Keys))

	if !params.FlushCache { // 从缓存中获取 mget
		if hits, err = ru.MGet(ctx, params.Keys, params.ResultSlice); err != nil {
			return err
		}
	}

	fallbackIndexes := make([]int, 0) // 缓存未命中的key

	for i, hited := range hits {
		if !hited {
			fallbackIndexes = append(fallbackIndexes, i)
		}
	}

	if len(fallbackIndexes) < 1 { // 全部命中缓存
		return nil
	}

	resultRFElem := reflect.ValueOf(params.ResultSlice).Elem() // 结果

	// 未命中缓存的处理 批量获取或并发获取
	err = ru.ruWrapperMgetFallbackHandle(ctx, fallbackIndexes, resultRFElem, params)

	return err
}

func (ru *RedisUtil) ruWrapperMgetFallbackHandle(ctx context.Context,
	fallbackIndexes []int, resultRFElem reflect.Value, params *WrapperParamsMget) (err error) {
	if params.BatchSetFunc != nil { // 批量获取
		var (
			batchDataInter interface{}
			err2           error
		)

		if params.SingleFlight {
			singleflightKey := strings.Join(params.Keys, ",")
			batchDataInter, err2, _ = ru.singleflightGroup(singleflightKey).
				Do(singleflightKey, func() (interface{}, error) {
					return ru.ruWrapperBatchCallAndSetCache(ctx, params, fallbackIndexes)
				})
		} else {
			batchDataInter, err2 = params.BatchSetFunc(fallbackIndexes)
		}

		if err2 != nil {
			return err2
		}

		batchData, ok := batchDataInter.(map[int]interface{}) // 类型断言
		if !ok {
			return errors.New(fmt.Sprintf("BatchSetFunc return value type error, %#v", batchData))
		}

		// 结果
		for _, fallbackIndex := range fallbackIndexes {
			if newData, ok := batchData[fallbackIndex]; ok {
				targetResultRFValue := resultRFElem.Index(fallbackIndex)
				targetResultRFValue.Set(reflect.ValueOf(newData))
			}
		}

		return nil
	}

	// 并发获取
	goGroup, _ := errgroup.WithContext(ctx)

	for _, fallbackIndex := range fallbackIndexes {
		fallbackIndex := fallbackIndex

		goGroup.Go(func() error {
			var err2 error

			var newData interface{}
			key := params.Keys[fallbackIndex]
			targetResultRFValue := resultRFElem.Index(fallbackIndex)

			if params.SingleFlight {
				newData, err2, _ = ru.singleflightGroup(key).Do(key, func() (interface{}, error) {
					return ru.ruWrapperCallAndSetCache(ctx, params, fallbackIndex)
				})
			} else {
				newData, err2 = ru.ruWrapperCallAndSetCache(ctx, params, fallbackIndex)
			}

			targetResultRFValue.Set(reflect.ValueOf(newData)) // 结果

			return err2
		})
	}

	return goGroup.Wait()
}

// 批量获取 fallback 函数调用和入缓存
func (ru *RedisUtil) ruWrapperBatchCallAndSetCache(ctx context.Context,
	params *WrapperParamsMget, fallbackIndexes []int) (map[int]interface{}, error) {
	batchData, err := params.BatchSetFunc(fallbackIndexes)
	if err != nil {
		return nil, err
	}

	setKeys := make([]string, 0)
	setValues := make([]interface{}, 0)
	setExpireSecondsSlice := make([]int, 0)

	for _, fallbackIndex := range fallbackIndexes {
		newData, ok := batchData[fallbackIndex]
		if !ok {
			continue
		}

		setKeys = append(setKeys, params.Keys[fallbackIndex])
		setValues = append(setValues, newData)
		setExpireSecondsSlice = append(setExpireSecondsSlice, params.ExpireSeconds[fallbackIndex])
	}

	if len(setKeys) > 0 {
		_ = ru.BatchSet(ctx, &BatchSetParams{
			Keys: setKeys, Values: setValues, ExpireSecondsSlice: setExpireSecondsSlice,
		})
	}

	return batchData, nil
}

// 并发获取 fallback 函数调用和入缓存
func (ru *RedisUtil) ruWrapperCallAndSetCache(ctx context.Context,
	params *WrapperParamsMget, fallbackIndex int) (interface{}, error) {
	key := params.Keys[fallbackIndex]
	expireSeconds := params.ExpireSeconds[fallbackIndex]
	setFunc := params.SetFuncSlice[fallbackIndex]

	data, err := setFunc(fallbackIndex)
	if err != nil {
		return nil, err
	}

	_ = ru.SetCache(ctx, key, data, expireSeconds)

	return data, nil
}

// 默认10组, 应该够用了，只是内存操作的lock
func (ru *RedisUtil) singleflightGroup(key string) *singleflight.Group {
	keyHash := base.CRC32(key)
	totalNum := ru.singleFlightGroupNum

	if totalNum < 1 {
		totalNum = DefaultSingleFlightGroupNum
	}

	return singleflight.GetGroup(fmt.Sprintf("CacheUtil:%d", keyHash%uint32(totalNum)))
}

func (ru *RedisUtil) DeleteCache(ctx context.Context, key string) (err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		_, err = conDo(ctx, con, "DEL", keyPatch(key))

		return err
	})

	return err
}

func (ru *RedisUtil) Expire(ctx context.Context, key string, ttl int) (err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		_, err = conDo(ctx, con, "EXPIRE", keyPatch(key), ttl)

		return err
	})

	return err
}

func (ru *RedisUtil) TTL(ctx context.Context, key string) (ttl int, err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		ttl, err = redis.Int(conDo(ctx, con, "TTL", keyPatch(key)))

		return err
	})

	return ttl, err
}

func (ru *RedisUtil) Incr(ctx context.Context, key string) (res int64, err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		res, err = redis.Int64(conDo(ctx, con, "INCR", keyPatch(key)))

		return err
	})

	return res, err
}

func (ru *RedisUtil) IncrBy(ctx context.Context, key string, diff int64) (res int64, err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		res, err = redis.Int64(conDo(ctx, con, "INCRBY", keyPatch(key), diff))

		return err
	})

	return res, err
}

func (ru *RedisUtil) Decr(ctx context.Context, key string) (res int64, err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		res, err = redis.Int64(conDo(ctx, con, "DECR", keyPatch(key)))

		return err
	})

	return res, err
}

func (ru *RedisUtil) DecrBy(ctx context.Context, key string, diff int64) (res int64, err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		res, err = redis.Int64(conDo(ctx, con, "DECRBY", keyPatch(key), diff))

		return err
	})

	return res, err
}

type SortSetInfo struct {
	Score int64
	Name  string
}

func (ru *RedisUtil) CacheZAdd(ctx context.Context, key string, infos []*SortSetInfo) (err error) {
	args := make([]interface{}, 0)
	args = append(args, keyPatch(key))

	for _, item := range infos {
		args = append(args, item.Score, item.Name)
	}

	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		_, err = conDo(ctx, con, "ZADD", args...)

		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (ru *RedisUtil) CacheZCard(ctx context.Context, key string) (res int64, err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		res, err = redis.Int64(conDo(ctx, con, "ZCARD", keyPatch(key)))

		return err
	})

	return res, err
}

func (ru *RedisUtil) CacheZRangeKeys(ctx context.Context, key string, start, end int) (result []string, err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		result, err = redis.Strings(conDo(ctx, con, "ZRANGE", keyPatch(key), start, end))

		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return result, err
	}

	return result, nil
}

func (ru *RedisUtil) CacheZRevRangeKeys(ctx context.Context, key string, start, end int) (result []string, err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		result, err = redis.Strings(conDo(ctx, con, "ZREVRANGE", keyPatch(key), start, end))

		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return result, err
	}

	return result, nil
}

func (ru *RedisUtil) CacheZrem(ctx context.Context, key string, names []string) (err error) {
	args := make([]interface{}, 0)
	args = append(args, keyPatch(key))

	for _, item := range names {
		args = append(args, item)
	}

	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		_, err = conDo(ctx, con, "ZREM", args...)

		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (ru *RedisUtil) WrapDo(ctx context.Context, doFunction func(con redis.Conn) error) error {
	con := ru.pool.Get()
	defer con.Close()

	return doFunction(con)
}

func (ru *RedisUtil) getLogger() Logger {
	if ru.logger != nil {
		return ru.logger
	}

	return GetDefaultLogger()
}