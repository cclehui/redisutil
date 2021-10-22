# redis-util
方便业务使用的redis操作封装，比如常见的缓存set get操作， 一行代码搞定,不像开源库需要写好多行

# 使用方法

```
import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/cclehui/redis-util"
)

server := ":6379"
password := "xxxxxxxxxx"

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

cacheUtil := NewCacheUtil(getTestClient())
redisUtil := NewRedisUtil(redisClient)
cacheKey := "cclehui_test_set_get_key_211022"

_ := redisUtil.Set(cacheKey, "axxxaa", 3600) // 设置缓存

valueStrRes := ""
_, _ = redisUtil.Get(cacheKey, &valueStrRes) // 获取缓存
fmt.Println("获取缓存:", valueStrRes)

_ = redisUtil.Del(cacheKey) // Del

value, _ := redisUtil.Incr(cacheKey)
fmt.Println("Incr:", value)

value, _ = redisUtil.Decr(cacheKey)
fmt.Println("Decr:", value)

```

