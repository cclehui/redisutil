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
)

type WrapperParams struct {
	Key           string
	ExpireSeconds int
	Result        interface{} // 结果
	FallbackFunc  FallbackFunc

	SingleFlight bool // 是否启动 singleflight
	FlushCache   bool // 是否用SetFunc刷新缓存
}

func (ru *RedisUtil) CacheWrapper(ctx context.Context,
	params *WrapperParams) (err error) {
	if !params.FlushCache {
		if hit, _ := ru.Get(ctx, params.Key, params.Result); hit {
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
	data, err := params.FallbackFunc()
	if err != nil {
		return nil, err
	}

	_ = ru.Set(ctx, params.Key, data, params.ExpireSeconds)

	return data, nil
}

type WrapperParamsMget struct {
	Keys          []string
	ExpireSeconds []int
	ResultSlice   interface{} // 结果 slice of your result

	// 批量或并发二选一
	FallbackFuncSlice []MgetFallbackFunc    // 并发获取
	BatchFallbackFunc MgetBatchFallbackFunc // 批量获取

	SingleFlight bool // 是否启动 singleflight
	FlushCache   bool // 用SetFunc刷新缓存
}

// 多key 缓存获取wrapper mget
func (ru *RedisUtil) CacheWrapperMget(ctx context.Context,
	params *WrapperParamsMget) (err error) {
	if len(params.Keys) != len(params.ExpireSeconds) {
		return errors.New("Keys, ExpireSeconds length should equal")
	}

	if len(params.FallbackFuncSlice) > 0 &&
		len(params.Keys) != len(params.FallbackFuncSlice) {
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
	if params.BatchFallbackFunc != nil { // 批量获取
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
			batchDataInter, err2 = params.BatchFallbackFunc(fallbackIndexes)
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

			if err2 != nil {
				return err2
			}

			targetResultRFValue.Set(reflect.ValueOf(newData)) // 结果

			return nil
		})
	}

	return goGroup.Wait()
}

// 批量获取 fallback 函数调用和入缓存
func (ru *RedisUtil) ruWrapperBatchCallAndSetCache(ctx context.Context,
	params *WrapperParamsMget, fallbackIndexes []int) (map[int]interface{}, error) {
	batchData, err := params.BatchFallbackFunc(fallbackIndexes)
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
	setFunc := params.FallbackFuncSlice[fallbackIndex]

	data, err := setFunc(fallbackIndex)
	if err != nil {
		return nil, err
	}

	_ = ru.Set(ctx, key, data, expireSeconds)

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
