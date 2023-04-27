//nolint:goconst
package redisutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetGet(t *testing.T) {
	ctx := context.Background()

	redisUtil := NewRedisUtil(getTestPool())
	redisKey := "gotest:cclehui_test_set_get_key_211022"

	value := 1

	err := redisUtil.Set(ctx, redisKey, value, 3600)
	assert.Equal(t, err, nil)

	_, _ = redisUtil.Get(ctx, redisKey, &value)
	assert.Equal(t, value, 1)

	err = redisUtil.Del(ctx, redisKey)
	assert.Equal(t, err, nil)
}

func TestCacheUtilSet(t *testing.T) {
	ctx := context.Background()

	redisUtil := NewRedisUtil(getTestPool())

	key1 := "gotest:redis_util:set1"

	data := "aaaaaaaaa"
	ttl := TTLNoExpire

	err := redisUtil.Set(ctx, key1, data, ttl)
	assert.Equal(t, nil, err)

	ttlFromRedis, err := redisUtil.TTL(ctx, key1)
	assert.Equal(t, nil, err)
	assert.Equal(t, TTLNoExpire, ttlFromRedis)

	ttl = 600

	err = redisUtil.Set(ctx, key1, data, ttl)
	assert.Equal(t, nil, err)

	ttlFromRedis, err = redisUtil.TTL(ctx, key1)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, TTLNoExpire, ttlFromRedis)

	_ = redisUtil.Del(ctx, key1)
}

func TestIncrDecr(t *testing.T) {
	ctx := context.Background()

	redisUtil := NewRedisUtil(getTestPool())
	redisKey := "gotest:cclehui_test_incr_decr_key_211022"

	_ = redisUtil.Del(ctx, redisKey)

	_ = redisUtil.Set(ctx, redisKey, 1, 3600)

	value, _ := redisUtil.Incr(ctx, redisKey)
	assert.Equal(t, value, int64(2))

	value, _ = redisUtil.Decr(ctx, redisKey)
	assert.Equal(t, value, int64(1))

	value, _ = redisUtil.IncrBy(ctx, redisKey, 10)
	assert.Equal(t, value, int64(11))

	value, _ = redisUtil.DecrBy(ctx, redisKey, 10)
	assert.Equal(t, value, int64(1))

	// _ = redisUtil.DeleteCache(ctx, redisKey)

	_ = redisUtil.Expire(ctx, redisKey, 600)

	ttl, _ := redisUtil.TTL(ctx, redisKey)
	if ttl < 0 || ttl > 600 {
		t.Fatalf("ttl时间异常, %d", ttl)
	}
}

func TestZSet(t *testing.T) {
	ctx := context.Background()

	redisUtil := NewRedisUtil(getTestPool())
	redisKey := "goteset:cclehui_test_zset"

	defer func() {
		_ = redisUtil.Del(ctx, redisKey)
	}()

	infos := []*SortSetInfo{
		{Score: 100, Name: "11111111"},
		{Score: 8, Name: "22222222"},
	}

	err := redisUtil.ZAdd(ctx, redisKey, infos)
	assert.Equal(t, nil, err)
	err = redisUtil.ZAdd(ctx, redisKey, infos)
	assert.Equal(t, nil, err)

	value, err := redisUtil.ZCard(ctx, redisKey)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(infos), int(value))

	deleteNames := make([]string, len(infos))
	for i, item := range infos {
		deleteNames[i] = item.Name
	}

	err = redisUtil.ZRem(ctx, redisKey, deleteNames)
	assert.Equal(t, nil, err)

	value, err = redisUtil.ZCard(ctx, redisKey)
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, int(value))
}

func TestBatchSet(t *testing.T) {
	ctx := context.Background()

	redisUtil := NewRedisUtil(getTestPool())

	key1 := "gotest:redis_util:TestCacheUtilBatchSet1"
	key2 := "gotest:redis_util:TestCacheUtilBatchSet2"

	data := "aaaaaaaaa"

	keys := []string{key1, key2}
	values := []interface{}{data, data}
	expireSecondsSlice := []int{600, 900}

	err := redisUtil.BatchSet(ctx, &BatchSetParams{
		Keys: keys, Values: values, ExpireSecondsSlice: expireSecondsSlice})
	assert.Equal(t, nil, err)

	mgetResult := make([]string, len(keys))

	hits, err := redisUtil.MGet(ctx, keys, &mgetResult)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(keys), len(hits))

	for i, hited := range hits {
		assert.Equal(t, true, hited)
		assert.Equal(t, data, mgetResult[i])
	}

	for _, key := range keys {
		_ = redisUtil.Del(ctx, key)
	}
}

func TestMGet(t *testing.T) {
	ctx := context.Background()
	redisUtil := NewRedisUtil(getTestPool())

	key1 := "gotest:redis_util:mget1"
	key2 := "gotest:redis_util:mget2"

	data := "aaaaaaaaa"
	ttl := 600

	keys := []string{key1, key2}
	length := 2
	mgetResult := make([]string, length)

	err := redisUtil.Set(ctx, keys[0], data, ttl)
	assert.Equal(t, nil, err)

	hits, err := redisUtil.MGet(ctx, keys, &mgetResult)
	assert.Equal(t, nil, err)

	assert.Equal(t, 2, len(hits))
	assert.Equal(t, true, hits[0])
	assert.Equal(t, false, hits[1])
	assert.Equal(t, data, mgetResult[0])
	assert.Equal(t, "", mgetResult[1])

	fmt.Printf("2222222222, %+v, %+v\n", hits, mgetResult)

	// 都没有获取到
	mgetResultNil := make([]string, length)
	hits, err = redisUtil.MGet(ctx, []string{"gotest:xxxxxxxxx", "gotest:yyyyyyy"}, &mgetResultNil)

	assert.Equal(t, nil, err)
	assert.Equal(t, false, hits[0])
	assert.Equal(t, false, hits[1])

	fmt.Printf("2222222222, %+v, %+v\n", hits, mgetResultNil)

	for _, key := range keys {
		_ = redisUtil.Del(ctx, key)
	}
}

func TestMGetStruct(t *testing.T) {
	ctx := context.Background()
	redisUtil := NewRedisUtil(getTestPool())

	key1 := "gotest:redis_util:mget1"
	key2 := "gotest:redis_util:mget2"

	type valueStruct struct {
		Name string
		Age  int
	}

	data := &valueStruct{Name: "TestCacheUtilMGetStructName", Age: 18}
	ttl := 600

	keys := []string{key1, key2}
	length := len(keys)

	err := redisUtil.Set(ctx, keys[0], data, ttl)
	assert.Equal(t, nil, err)

	mgetResult := make([]valueStruct, length) // 非指针形式

	hits, err := redisUtil.MGet(ctx, keys, &mgetResult)
	assert.Equal(t, nil, err)

	assert.Equal(t, len(keys), len(hits))
	assert.Equal(t, true, hits[0])
	assert.Equal(t, false, hits[1])
	assert.Equal(t, data.Name, mgetResult[0].Name)
	assert.Equal(t, data.Age, mgetResult[0].Age)
	assert.Equal(t, "", mgetResult[1].Name)
	assert.Equal(t, 0, mgetResult[1].Age)

	mgetResult2 := make([]*valueStruct, length) // 指针形式

	hits2, err2 := redisUtil.MGet(ctx, keys, &mgetResult2)
	assert.Equal(t, nil, err2)

	assert.Equal(t, len(keys), len(hits2))
	assert.Equal(t, true, hits2[0])
	assert.Equal(t, false, hits2[1])
	assert.Equal(t, data.Name, mgetResult2[0].Name)
	assert.Equal(t, data.Age, mgetResult2[0].Age)
	assert.Nil(t, mgetResult2[1])

	for _, key := range keys {
		_ = redisUtil.Del(ctx, key)
	}
}
