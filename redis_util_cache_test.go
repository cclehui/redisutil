//nolint:goconst
package redisutil

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestCacheWrapper(t *testing.T) {
	ctx := context.Background()

	redisUtil := NewRedisUtil(getTestPool())
	key := "gotest:redis_wrapper:get"

	type valueStruct struct {
		Name string
		Age  int
	}

	age := 18
	name := "TestName"

	logPrefix := "CacheWrapper: 基本特征"

	setFunc := func() (interface{}, error) {
		fmt.Printf("%s, ccccccccc call real function\n", logPrefix)
		time.Sleep(time.Second * 1)

		data := &valueStruct{
			Name: name,
			Age:  age,
		}

		return data, nil
	}

	testParams := []*WrapperParams{
		{
			Key: key, ExpireSeconds: 600, FallbackFunc: setFunc,
			SingleFlight: false, FlushCache: false,
		},
		{
			Key: key, ExpireSeconds: 600, FallbackFunc: setFunc,
			SingleFlight: true, FlushCache: false,
		},
		{
			Key: key, ExpireSeconds: 600, FallbackFunc: setFunc,
			SingleFlight: false, FlushCache: true,
		},
		{
			Key: key, ExpireSeconds: 600, FallbackFunc: setFunc,
			SingleFlight: true, FlushCache: true,
		},
	}

	runFunc := func(params *WrapperParams, index int) {
		n := 10
		dataResult := make([]*valueStruct, n)

		defer func() {
			_ = redisUtil.Del(ctx, key)
		}()

		goGroup, _ := errgroup.WithContext(ctx)

		for j := 0; j < n; j++ { // N个并发
			j := j
			paramsTemp := &WrapperParams{
				Key:           params.Key,
				ExpireSeconds: params.ExpireSeconds,
				FallbackFunc:  params.FallbackFunc,
				Result:        &dataResult[j],

				SingleFlight: params.SingleFlight,
				FlushCache:   params.FlushCache,
			}
			paramsTemp.Result = &dataResult[j]

			goGroup.Go(func() error {
				return redisUtil.CacheWrapper(ctx, paramsTemp)
			})
		}

		err2 := goGroup.Wait()
		assert.Equal(t, nil, err2)

		resultLogPtr, _ := json.Marshal(dataResult)
		fmt.Printf("%s, 结果, index:%d, SingleFlight:%+v, %+v, %s\n",
			logPrefix, index, params.SingleFlight, dataResult, string(resultLogPtr))

		for j := 0; j < n; j++ {
			assert.Equal(t, dataResult[j].Name, name)
			assert.Equal(t, dataResult[j].Age, age)
		}

		if params.SingleFlight {
			for j := 1; j < n; j++ {
				assert.Equal(t, dataResult[0], dataResult[j])
			}
		}
	}

	for i, params := range testParams {
		runFunc(params, i)
	}
}

func TestCacheWrapperMgetGoroutine(t *testing.T) {
	ctx := context.Background()

	redisUtil := NewRedisUtil(getTestPool())

	key1 := "gotest:redis_util:mget1"
	key2 := "gotest:redis_util:mget2"

	type valueStruct struct {
		Name          string
		Age           int
		Index         int
		FallbackIndex int
	}

	keys := []string{key1, key2}
	expireSeconds := []int{600, 900}
	length := 2

	logPrefix := "CacheWrapperMget, 并发单次获取"

	setFuncSlice := make([]MgetFallbackFunc, len(keys)) // 实际调用的函数

	for i := range keys {
		i := i
		setFuncSlice[i] = func(fallbackIndex int) (interface{}, error) {
			fmt.Printf("%s, ccccccccc call real function", logPrefix)
			time.Sleep(time.Second * 1)

			data := &valueStruct{
				Name:          "TestCacheUtilMGetStructName",
				Age:           18,
				Index:         i,
				FallbackIndex: fallbackIndex,
			}

			return data, nil
		}
	}

	// 调用两次  第一次穿透， 第二次部分穿透
	for i := 0; i < 2; i++ {
		dataResult := make([]*valueStruct, length) // 指针形式

		err := redisUtil.CacheWrapperMget(ctx, &WrapperParamsMget{
			Keys:              keys,
			ExpireSeconds:     expireSeconds,
			ResultSlice:       &dataResult,
			FallbackFuncSlice: setFuncSlice,
		})

		assert.Equal(t, nil, err)

		for i := range keys {
			assert.Equal(t, i, dataResult[i].Index)
			assert.Equal(t, i, dataResult[i].FallbackIndex)
		}

		resultLogPtr, _ := json.Marshal(dataResult)

		fmt.Printf("%s, %+v, %s\n", logPrefix, dataResult, string(resultLogPtr))

		if i == 0 {
			_ = redisUtil.Del(ctx, key1)
		}
	}

	for _, key := range keys {
		_ = redisUtil.Del(ctx, key)
	}
}

func TestCacheWrapperMgetGoroutineSingleFlight(t *testing.T) {
	ctx := context.Background()

	redisUtil := NewRedisUtil(getTestPool())

	key1 := "gotest:redis_util:mget1"
	key2 := "gotest:redis_util:mget2"

	type valueStruct struct {
		Name          string
		Age           int
		Index         int
		FallbackIndex int
	}

	keys := []string{key1, key2}
	expireSeconds := []int{600, 900}
	length := 2

	logPrefix := "CacheWrapperMget SingleFlight, 并发单次获取"

	setFuncSlice := make([]MgetFallbackFunc, len(keys)) // 实际调用的函数

	for i := range keys {
		i := i
		setFuncSlice[i] = func(fallbackIndex int) (interface{}, error) {
			nowStr := time.Now().Format("2006-01-02 15:04:05")
			fmt.Printf("%s, ccccccccc call real function, %s, %d\n", logPrefix, nowStr, fallbackIndex)
			time.Sleep(time.Second * 1)

			data := &valueStruct{
				Name:          "TestCacheUtilMGetStructName",
				Age:           18,
				Index:         i,
				FallbackIndex: fallbackIndex,
			}

			return data, nil
		}
	}

	// 调用两次  第一次穿透， 第二次部分穿透
	for i := 0; i < 2; i++ {
		i := i
		n := 10
		dataResult := make([][]*valueStruct, n)

		for j := 0; j < n; j++ { // N个并发
			dataResult[j] = make([]*valueStruct, length) // 指针形式
		}

		goGroup, _ := errgroup.WithContext(ctx)

		for j := 0; j < n; j++ { // N个并发
			j := j

			goGroup.Go(func() error {
				return redisUtil.CacheWrapperMget(ctx, &WrapperParamsMget{
					Keys:              keys,
					ExpireSeconds:     expireSeconds,
					ResultSlice:       &dataResult[j],
					FallbackFuncSlice: setFuncSlice,

					SingleFlight: true,
				})
			})
		}

		err2 := goGroup.Wait()
		assert.Equal(t, nil, err2)

		resultLogPtr, _ := json.Marshal(dataResult)
		fmt.Printf("%s, %+v, %s\n", logPrefix, dataResult, string(resultLogPtr))

		for j := 1; j < n; j++ {
			assert.Equal(t, dataResult[0], dataResult[j])
		}

		if i == 0 {
			_ = redisUtil.Del(ctx, key1)
		}
	}

	for _, key := range keys {
		_ = redisUtil.Del(ctx, key)
	}
}

func TestCacheWrapperMgetBatch(t *testing.T) {
	ctx := context.Background()

	redisUtil := NewRedisUtil(getTestPool())

	key1 := "gotest:redis_util:mget1"
	key2 := "gotest:redis_util:mget2"

	type valueStruct struct {
		Name          string
		Age           int
		Index         int
		FallbackIndex int
	}

	keys := []string{key1, key2}
	expireSeconds := []int{600, 900}
	length := 2

	logPrefix := "CacheWrapperMget, 批量获取"

	batchSetFunc := func(fallbackIndexes []int) (map[int]interface{}, error) {
		fmt.Printf("%s, ddddddddd call real function, fallbackIndexes:%+v\n", logPrefix, fallbackIndexes)
		time.Sleep(time.Second * 1)

		result := make(map[int]interface{})

		for _, i := range fallbackIndexes {
			result[i] = &valueStruct{
				Name:          "TestCacheUtilMGetStructName",
				Age:           18,
				Index:         i,
				FallbackIndex: i,
			}
		}

		return result, nil
	}

	// 调用两次  第一次穿透， 第二次部分穿透
	for i := 0; i < 2; i++ {
		dataResult := make([]*valueStruct, length) // 指针形式

		err := redisUtil.CacheWrapperMget(ctx, &WrapperParamsMget{
			Keys:              keys,
			ExpireSeconds:     expireSeconds,
			ResultSlice:       &dataResult,
			BatchFallbackFunc: batchSetFunc,
		})

		assert.Equal(t, nil, err)

		for i := range keys {
			assert.Equal(t, i, dataResult[i].Index)
			assert.Equal(t, i, dataResult[i].FallbackIndex)
		}

		resultLogPtr, _ := json.Marshal(dataResult)

		fmt.Printf("%s,获取结果, %+v, %s\n", logPrefix, dataResult, string(resultLogPtr))

		if i == 0 {
			_ = redisUtil.Del(ctx, key1)
		}
	}

	for _, key := range keys {
		_ = redisUtil.Del(ctx, key)
	}
}

func TestCacheWrapperMgetBatchSingleFlight(t *testing.T) {
	ctx := context.Background()

	redisUtil := NewRedisUtil(getTestPool())

	key1 := "gotest:redis_util:mget1"
	key2 := "gotest:redis_util:mget2"

	type valueStruct struct {
		Name          string
		Age           int
		Index         int
		FallbackIndex int
	}

	keys := []string{key1, key2}
	expireSeconds := []int{600, 900}
	length := 2

	logPrefix := "CacheWrapperMget SingleFlight, 批量获取"

	//nolint:unparam
	batchSetFunc := func(fallbackIndexes []int) (map[int]interface{}, error) {
		fmt.Printf("%s, ddddddddd call real function, fallbackIndexes:%+v\n", logPrefix, fallbackIndexes)
		time.Sleep(time.Second * 1)

		result := make(map[int]interface{})

		for _, i := range fallbackIndexes {
			result[i] = &valueStruct{
				Name:          "TestCacheUtilMGetStructName",
				Age:           18,
				Index:         i,
				FallbackIndex: i,
			}
		}

		return result, nil
	}

	// 调用两次  第一次穿透， 第二次部分穿透
	for i := 0; i < 2; i++ {
		i := i
		n := 10
		dataResult := make([][]*valueStruct, n)

		for j := 0; j < n; j++ { // N个并发
			dataResult[j] = make([]*valueStruct, length) // 指针形式
		}

		goGroup, _ := errgroup.WithContext(ctx)

		for j := 0; j < n; j++ { // N个并发
			j := j

			goGroup.Go(func() error {
				return redisUtil.CacheWrapperMget(ctx, &WrapperParamsMget{
					Keys:              keys,
					ExpireSeconds:     expireSeconds,
					ResultSlice:       &dataResult[j],
					BatchFallbackFunc: batchSetFunc,

					SingleFlight: true,
				})
			})
		}

		err2 := goGroup.Wait()
		assert.Equal(t, nil, err2)

		resultLogPtr, _ := json.Marshal(dataResult)
		fmt.Printf("%s, %+v, %s\n", logPrefix, dataResult, string(resultLogPtr))

		for j := 1; j < n; j++ {
			assert.Equal(t, dataResult[0], dataResult[j])
		}

		if i == 0 {
			_ = redisUtil.Del(ctx, key1)
		}
	}

	for _, key := range keys {
		_ = redisUtil.Del(ctx, key)
	}
}
