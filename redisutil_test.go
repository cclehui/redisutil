package redisutil

import (
	"context"
	"sync"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

var redisClient *redis.Pool
var redisClientOnce sync.Once
var ctx = context.Background()

func getTestClient() *redis.Pool {
	server := "127.0.0.1:6379"
	password := "123456"

	redisClientOnce.Do(func() {
		redisClient = &redis.Pool{
			// Other pool configuration not shown in this example.
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", server)
				if err != nil {
					return nil, err
				}

				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}

				/*
					if _, err := c.Do("SELECT", db); err != nil {
						c.Close()
						return nil, err
					}
				*/
				return c, nil
			},
		}
	})

	return redisClient
}

type testStruct struct {
	Name string
	Age  int
}

func TestSetGet(t *testing.T) {
	redisUtil := NewRedisUtil(getTestClient())
	cacheKey := "cclehui_test_set_get_key_211022"

	// 整形测试
	value := 1

	err := redisUtil.Set(ctx, cacheKey, value, 3600)
	assert.Equal(t, err, nil)

	_, _ = redisUtil.Get(ctx, cacheKey, &value)
	assert.Equal(t, value, 1)

	// 字符串测试
	valueStr := "adfasf&%%^*(我哈哈哈哈啊啊}{）*&……&"

	err = redisUtil.Set(ctx, cacheKey, valueStr, 3600)
	assert.Equal(t, err, nil)

	valueStrRes := ""
	_, _ = redisUtil.Get(ctx, cacheKey, &valueStrRes)
	assert.Equal(t, valueStr, valueStrRes)

	// struct 测试
	cclehui := &testStruct{
		Name: "cclehui_test",
		Age:  18,
	}

	cclehuiRes := &testStruct{}

	_ = redisUtil.Set(ctx, cacheKey, cclehui, 3600)
	_, _ = redisUtil.Get(ctx, cacheKey, cclehuiRes)
	assert.Equal(t, cclehui, cclehuiRes)

	// map 测试
	mapTest := map[string]interface{}{
		"name": "cclehui_test_map",
		"age":  18,
	}

	mapTestRes := make(map[string]interface{})

	_ = redisUtil.Set(ctx, cacheKey, mapTest, 3600)
	_, _ = redisUtil.Get(ctx, cacheKey, &mapTestRes)
	assert.Equal(t, mapTest, mapTestRes)

	// 删除
	err = redisUtil.Del(ctx, cacheKey)
	assert.Equal(t, err, nil)
}

func TestIncrDecr(t *testing.T) {
	redisUtil := NewRedisUtil(getTestClient())
	cacheKey := "cclehui_test_incr_decr_key_211022"

	_ = redisUtil.Del(ctx, cacheKey)

	_ = redisUtil.Set(ctx, cacheKey, 1, 3600)

	value, _ := redisUtil.Incr(ctx, cacheKey)
	assert.Equal(t, value, int64(2))

	value, _ = redisUtil.Decr(ctx, cacheKey)
	assert.Equal(t, value, int64(1))

	value, _ = redisUtil.IncrBy(ctx, cacheKey, 10)
	assert.Equal(t, value, int64(11))

	value, _ = redisUtil.DecrBy(ctx, cacheKey, 10)
	assert.Equal(t, value, int64(1))

	// _ = redisUtil.DeleteCache(cacheKey)

	_ = redisUtil.Expire(ctx, cacheKey, 600)

	ttl, _ := redisUtil.TTL(ctx, cacheKey)
	if ttl < 0 || ttl > 600 {
		t.Fatalf("ttl时间异常, %d", ttl)
	}
}
