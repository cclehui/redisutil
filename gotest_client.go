package redisutil

import (
	"fmt"
	"sync"

	"github.com/cclehui/redisutil/internal/test"
	"github.com/gomodule/redigo/redis"
)

// gotest 所用的client

var gotestRedisPool *redis.Pool
var gotestRedisPoolOnce sync.Once

func getTestPool() *redis.Pool {
	gotestRedisPoolOnce.Do(func() {
		server := fmt.Sprintf("%s:%d", test.Conf.Redis.Endpoint.Address, test.Conf.Redis.Endpoint.Port)
		gotestRedisPool = &redis.Pool{
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", server)
				if err != nil {
					return nil, err
				}

				if test.Conf.Redis.Auth != "" {
					if _, err := c.Do("AUTH", test.Conf.Redis.Auth); err != nil {
						c.Close()
						return nil, err
					}
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

	return gotestRedisPool
}
