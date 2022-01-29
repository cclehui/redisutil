package main

import (
	"context"
	"fmt"

	redisutil "github.com/cclehui/redis-util"
	"github.com/gomodule/redigo/redis"
)

func main() {
	server := "127.0.0.1:6379"
	password := "123456"
	ctx := context.Background()

	redisClient := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}

			if _, err := c.Do("AUTH", password); err != nil {
				c.Close()
				return nil, err
			}

			return c, nil
		},
	}

	redisUtil := redisutil.NewRedisUtil(redisClient)
	cacheKey := "cclehui_test_set_get_key_211022"

	_ = redisUtil.Set(ctx, cacheKey, "axxxaa", 3600) // 设置缓存

	valueStrRes := ""
	_, _ = redisUtil.Get(ctx, cacheKey, &valueStrRes) // 获取缓存
	fmt.Println("获取缓存:", valueStrRes)

	_ = redisUtil.Del(ctx, cacheKey) // Del

	value, _ := redisUtil.Incr(ctx, cacheKey)
	fmt.Println("Incr:", value)

	value, _ = redisUtil.Decr(ctx, cacheKey)
	fmt.Println("Decr:", value)
}
