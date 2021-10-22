package main

import (
	"fmt"

	redisutil "github.com/cclehui/redis-util"
	"github.com/gomodule/redigo/redis"
)

func main() {
	server := "xxxxx:6379"
	password := "wxxxxxxxxx"

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

	_ = redisUtil.Set(cacheKey, "axxxaa", 3600) // 设置缓存

	valueStrRes := ""
	_, _ = redisUtil.Get(cacheKey, &valueStrRes) // 获取缓存
	fmt.Println("获取缓存:", valueStrRes)

	_ = redisUtil.Del(cacheKey) // Del

	value, _ := redisUtil.Incr(cacheKey)
	fmt.Println("Incr:", value)

	value, _ = redisUtil.Decr(cacheKey)
	fmt.Println("Decr:", value)
}
