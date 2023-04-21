//nolint:goconst
package redisutil

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"git2.qingtingfm.com/infra/qt-boot/pkg/goroutine"
	"github.com/stretchr/testify/assert"
)

func TestSetGet(t *testing.T) {
	ctx := context.Background()

	cacheUtil := NewCacheUtil(getTestPool())
	cacheKey := "cclehui_test_set_get_key_211022"

	value := 1

	err := cacheUtil.SetCache(ctx, cacheKey, value, 3600)
	assert.Equal(t, err, nil)

	_, _ = cacheUtil.GetCache(ctx, cacheKey, &value)
	assert.Equal(t, value, 1)

	err = cacheUtil.DeleteCache(ctx, cacheKey)
	assert.Equal(t, err, nil)
}

func TestCacheUtilSet(t *testing.T) {
	ctx := context.Background()

	cacheUtil := NewCacheUtil(getTestPool())

	key1 := "gotest:cache_util:set1"

	data := "aaaaaaaaa"
	ttl := TTLNoExpire

	err := cacheUtil.SetCache(ctx, key1, data, ttl)
	assert.Equal(t, nil, err)

	ttlFromRedis, err := cacheUtil.TTL(ctx, key1)
	assert.Equal(t, nil, err)
	assert.Equal(t, TTLNoExpire, ttlFromRedis)

	ttl = 600

	err = cacheUtil.SetCache(ctx, key1, data, ttl)
	assert.Equal(t, nil, err)

	ttlFromRedis, err = cacheUtil.TTL(ctx, key1)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, TTLNoExpire, ttlFromRedis)

	_ = cacheUtil.DeleteCache(ctx, key1)
}

func TestIncrDecr(t *testing.T) {
	ctx := context.Background()

	cacheUtil := NewCacheUtil(getTestPool())
	cacheKey := "cclehui_test_incr_decr_key_211022"

	_ = cacheUtil.DeleteCache(ctx, cacheKey)

	_ = cacheUtil.SetCache(ctx, cacheKey, 1, 3600)

	value, _ := cacheUtil.Incr(ctx, cacheKey)
	assert.Equal(t, value, int64(2))

	value, _ = cacheUtil.Decr(ctx, cacheKey)
	assert.Equal(t, value, int64(1))

	value, _ = cacheUtil.IncrBy(ctx, cacheKey, 10)
	assert.Equal(t, value, int64(11))

	value, _ = cacheUtil.DecrBy(ctx, cacheKey, 10)
	assert.Equal(t, value, int64(1))

	// _ = cacheUtil.DeleteCache(ctx, cacheKey)

	_ = cacheUtil.Expire(ctx, cacheKey, 600)

	ttl, _ := cacheUtil.TTL(ctx, cacheKey)
	if ttl < 0 || ttl > 600 {
		t.Fatalf("ttl时间异常, %d", ttl)
	}
}

func TestZSet(t *testing.T) {
	ctx := context.Background()

	cacheUtil := NewCacheUtil(getTestPool())
	cacheKey := "cclehui_test_zset"

	defer func() {
		_ = cacheUtil.DeleteCache(ctx, cacheKey)
	}()

	infos := []*SortSetInfo{
		{Score: 100, Name: "11111111"},
		{Score: 8, Name: "22222222"},
	}

	err := cacheUtil.CacheZAdd(ctx, cacheKey, infos)
	assert.Equal(t, nil, err)
	err = cacheUtil.CacheZAdd(ctx, cacheKey, infos)
	assert.Equal(t, nil, err)

	value, err := cacheUtil.CacheZCard(ctx, cacheKey)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(infos), int(value))

	deleteNames := make([]string, len(infos))
	for i, item := range infos {
		deleteNames[i] = item.Name
	}

	err = cacheUtil.CacheZrem(ctx, cacheKey, deleteNames)
	assert.Equal(t, nil, err)

	value, err = cacheUtil.CacheZCard(ctx, cacheKey)
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, int(value))

}

func TestBatchSet(t *testing.T) {
	ctx := context.Background()

	cacheUtil := NewCacheUtil(getTestPool())

	key1 := "gotest:cache_util:TestCacheUtilBatchSet1"
	key2 := "gotest:cache_util:TestCacheUtilBatchSet2"

	data := "aaaaaaaaa"

	keys := []string{key1, key2}
	values := []interface{}{data, data}
	expireSecondsSlice := []int{600, 900}

	err := cacheUtil.BatchSet(ctx, &BatchSetParams{
		Keys: keys, Values: values, ExpireSecondsSlice: expireSecondsSlice})
	assert.Equal(t, nil, err)

	mgetResult := make([]string, len(keys))

	hits, err := cacheUtil.MGet(ctx, keys, &mgetResult)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(keys), len(hits))

	for i, hited := range hits {
		assert.Equal(t, true, hited)
		assert.Equal(t, data, mgetResult[i])
	}

	for _, key := range keys {
		_ = cacheUtil.DeleteCache(ctx, key)
	}
}

func TestMGet(t *testing.T) {
	ctx := context.Background()
	cacheUtil := NewCacheUtil(getTestPool())

	key1 := "gotest:cache_util:mget1"
	key2 := "gotest:cache_util:mget2"

	data := "aaaaaaaaa"
	ttl := 600

	keys := []string{key1, key2}
	length := 2
	mgetResult := make([]string, length)

	err := cacheUtil.SetCache(ctx, keys[0], data, ttl)
	assert.Equal(t, nil, err)

	hits, err := cacheUtil.MGet(ctx, keys, &mgetResult)
	assert.Equal(t, nil, err)

	assert.Equal(t, 2, len(hits))
	assert.Equal(t, true, hits[0])
	assert.Equal(t, false, hits[1])
	assert.Equal(t, data, mgetResult[0])
	assert.Equal(t, "", mgetResult[1])

	fmt.Printf("2222222222, %+v, %+v\n", hits, mgetResult)

	// 都没有获取到
	mgetResultNil := make([]string, length)
	hits, err = cacheUtil.MGet(ctx, []string{"gotest:xxxxxxxxx", "gotest:yyyyyyy"}, &mgetResultNil)

	assert.Equal(t, nil, err)
	assert.Equal(t, false, hits[0])
	assert.Equal(t, false, hits[1])

	fmt.Printf("2222222222, %+v, %+v\n", hits, mgetResultNil)

	for _, key := range keys {
		_ = cacheUtil.DeleteCache(ctx, key)
	}
}

func TestMGetStruct(t *testing.T) {
	ctx := context.Background()
	cacheUtil := NewCacheUtil(getTestPool())

	key1 := "gotest:cache_util:mget1"
	key2 := "gotest:cache_util:mget2"

	type valueStruct struct {
		Name string
		Age  int
	}

	data := &valueStruct{Name: "TestCacheUtilMGetStructName", Age: 18}
	ttl := 600

	keys := []string{key1, key2}
	length := len(keys)

	err := cacheUtil.SetCache(ctx, keys[0], data, ttl)
	assert.Equal(t, nil, err)

	mgetResult := make([]valueStruct, length) // 非指针形式

	hits, err := cacheUtil.MGet(ctx, keys, &mgetResult)
	assert.Equal(t, nil, err)

	assert.Equal(t, len(keys), len(hits))
	assert.Equal(t, true, hits[0])
	assert.Equal(t, false, hits[1])
	assert.Equal(t, data.Name, mgetResult[0].Name)
	assert.Equal(t, data.Age, mgetResult[0].Age)
	assert.Equal(t, "", mgetResult[1].Name)
	assert.Equal(t, 0, mgetResult[1].Age)

	mgetResult2 := make([]*valueStruct, length) // 指针形式

	hits2, err2 := cacheUtil.MGet(ctx, keys, &mgetResult2)
	assert.Equal(t, nil, err2)

	assert.Equal(t, len(keys), len(hits2))
	assert.Equal(t, true, hits2[0])
	assert.Equal(t, false, hits2[1])
	assert.Equal(t, data.Name, mgetResult2[0].Name)
	assert.Equal(t, data.Age, mgetResult2[0].Age)
	assert.Nil(t, mgetResult2[1])

	for _, key := range keys {
		_ = cacheUtil.DeleteCache(ctx, key)
	}
}

func TestCacheWrapper(t *testing.T) {
	ctx := context.Background()

	cacheUtil := NewCacheUtil(getTestPool())
	key := "gotest:cache_wrapper:get"

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
			Key: key, ExpireSeconds: 600, SetFunc: setFunc,
			SingleFlight: false, FlushCache: false,
		},
		{
			Key: key, ExpireSeconds: 600, SetFunc: setFunc,
			SingleFlight: true, FlushCache: false,
		},
		{
			Key: key, ExpireSeconds: 600, SetFunc: setFunc,
			SingleFlight: false, FlushCache: true,
		},
		{
			Key: key, ExpireSeconds: 600, SetFunc: setFunc,
			SingleFlight: true, FlushCache: true,
		},
	}

	runFunc := func(params *WrapperParams, index int) {
		n := 10
		dataResult := make([]*valueStruct, n)

		defer func() {
			_ = cacheUtil.DeleteCache(ctx, key)
		}()

		goGroup := goroutine.New("CacheUtilCacheWrapper")

		for j := 0; j < n; j++ { // N个并发
			goName := fmt.Sprintf("CacheUtilCacheWrapper_%d", j)
			j := j
			paramsTemp := &WrapperParams{
				Key:           params.Key,
				ExpireSeconds: params.ExpireSeconds,
				SetFunc:       params.SetFunc,
				Result:        &dataResult[j],

				SingleFlight: params.SingleFlight,
				FlushCache:   params.FlushCache,
			}
			paramsTemp.Result = &dataResult[j]

			goGroup.Go(ctx, goName, func(ctx context.Context) error {
				return cacheUtil.CacheWrapper(ctx, paramsTemp)
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

	cacheUtil := NewCacheUtil(getTestPool())

	key1 := "gotest:cache_util:mget1"
	key2 := "gotest:cache_util:mget2"

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

	setFuncSlice := make([]MgetSetFunc, len(keys)) // 实际调用的函数

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

		err := cacheUtil.CacheWrapperMget(ctx, &WrapperParamsMget{
			Keys:          keys,
			ExpireSeconds: expireSeconds,
			ResultSlice:   &dataResult,
			SetFuncSlice:  setFuncSlice,

			GoroutineOptions: []goroutine.Option{goroutine.SetMaxWorker(5, true)},
		})

		assert.Equal(t, nil, err)

		for i := range keys {
			assert.Equal(t, i, dataResult[i].Index)
			assert.Equal(t, i, dataResult[i].FallbackIndex)
		}

		resultLogPtr, _ := json.Marshal(dataResult)

		fmt.Printf("%s, %+v, %s\n", logPrefix, dataResult, string(resultLogPtr))

		if i == 0 {
			_ = cacheUtil.DeleteCache(ctx, key1)
		}
	}

	for _, key := range keys {
		_ = cacheUtil.DeleteCache(ctx, key)
	}
}

func TestCacheWrapperMgetGoroutineSingleFlight(t *testing.T) {
	ctx := context.Background()

	cacheUtil := NewCacheUtil(getTestPool())

	key1 := "gotest:cache_util:mget1"
	key2 := "gotest:cache_util:mget2"

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

	setFuncSlice := make([]MgetSetFunc, len(keys)) // 实际调用的函数

	for i := range keys {
		i := i
		setFuncSlice[i] = func(fallbackIndex int) (interface{}, error) {
			fmt.Printf("%s, ccccccccc call real function, %d\n", logPrefix, fallbackIndex)
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

		goGroup := goroutine.New("CacheWrapperMgetGoroutineSingleFlight")

		for j := 0; j < n; j++ { // N个并发
			goName := fmt.Sprintf("CacheWrapperMgetGoroutineSingleFlight_%d", j)
			j := j

			goGroup.Go(ctx, goName, func(ctx context.Context) error {
				return cacheUtil.CacheWrapperMget(ctx, &WrapperParamsMget{
					Keys:          keys,
					ExpireSeconds: expireSeconds,
					ResultSlice:   &dataResult[j],
					SetFuncSlice:  setFuncSlice,

					SingleFlight:     true,
					GoroutineOptions: []goroutine.Option{goroutine.SetMaxWorker(5, true)},
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
			_ = cacheUtil.DeleteCache(ctx, key1)
		}
	}

	for _, key := range keys {
		_ = cacheUtil.DeleteCache(ctx, key)
	}
}

func TestCacheWrapperMgetBatch(t *testing.T) {
	ctx := context.Background()

	cacheUtil := NewCacheUtil(getTestPool())

	key1 := "gotest:cache_util:mget1"
	key2 := "gotest:cache_util:mget2"

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

		err := cacheUtil.CacheWrapperMget(ctx, &WrapperParamsMget{
			Keys:          keys,
			ExpireSeconds: expireSeconds,
			ResultSlice:   &dataResult,
			BatchSetFunc:  batchSetFunc,
		})

		assert.Equal(t, nil, err)

		for i := range keys {
			assert.Equal(t, i, dataResult[i].Index)
			assert.Equal(t, i, dataResult[i].FallbackIndex)
		}

		resultLogPtr, _ := json.Marshal(dataResult)

		fmt.Printf("%s,获取结果, %+v, %s\n", logPrefix, dataResult, string(resultLogPtr))

		if i == 0 {
			_ = cacheUtil.DeleteCache(ctx, key1)
		}
	}

	for _, key := range keys {
		_ = cacheUtil.DeleteCache(ctx, key)
	}
}

func TestCacheWrapperMgetBatchSingleFlight(t *testing.T) {
	ctx := context.Background()

	cacheUtil := NewCacheUtil(getTestPool())

	key1 := "gotest:cache_util:mget1"
	key2 := "gotest:cache_util:mget2"

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

		goGroup := goroutine.New("CacheWrapperMgetGoroutineSingleFlight")

		for j := 0; j < n; j++ { // N个并发
			goName := fmt.Sprintf("CacheWrapperMgetGoroutineSingleFlight_%d", j)
			j := j

			goGroup.Go(ctx, goName, func(ctx context.Context) error {
				return cacheUtil.CacheWrapperMget(ctx, &WrapperParamsMget{
					Keys:          keys,
					ExpireSeconds: expireSeconds,
					ResultSlice:   &dataResult[j],
					BatchSetFunc:  batchSetFunc,

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
			_ = cacheUtil.DeleteCache(ctx, key1)
		}
	}

	for _, key := range keys {
		_ = cacheUtil.DeleteCache(ctx, key)
	}
}
