package redisutil

import (
	"context"
	"fmt"
	"reflect"

	"github.com/cclehui/redisutil/internal/base"
	"github.com/pkg/errors"

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

func (ru *RedisUtil) Set(ctx context.Context, key string, value interface{}, ttl int) (err error) {
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

func (ru *RedisUtil) Get(ctx context.Context, key string, value interface{}) (hit bool, err error) {
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

func (ru *RedisUtil) Del(ctx context.Context, key string) (err error) {
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

func (ru *RedisUtil) ZAdd(ctx context.Context, key string, infos []*SortSetInfo) (err error) {
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

func (ru *RedisUtil) ZCard(ctx context.Context, key string) (res int64, err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		res, err = redis.Int64(conDo(ctx, con, "ZCARD", keyPatch(key)))

		return err
	})

	return res, err
}

func (ru *RedisUtil) ZRange(ctx context.Context, key string, start, end int) (result []string, err error) {
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

func (ru *RedisUtil) ZRevRange(ctx context.Context, key string, start, end int) (result []string, err error) {
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

func (ru *RedisUtil) ZRem(ctx context.Context, key string, names []string) (err error) {
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
