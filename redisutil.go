package redisutil

import (
	"context"
	"reflect"

	"github.com/pkg/errors"

	"github.com/gomodule/redigo/redis"
)

type RedisUtil struct {
	pool *redis.Pool
}

func NewRedisUtil(pool *redis.Pool) *RedisUtil {
	return &RedisUtil{pool}
}

func (ru *RedisUtil) Set(ctx context.Context, key string, value interface{}, ttl int) (err error) {
	var bytesData []byte

	// 判断是否整数
	if s, ok := isNum(value); ok {
		bytesData = []byte(s)
	} else {
		bytesData, err = Encode(value)

		if err != nil {
			return err
		}
	}

	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		_, err = con.Do("SET", keyPatch(key), bytesData, "EX", ttl)
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

func (ru *RedisUtil) Get(ctx context.Context, key string, value interface{}) (hit bool, err error) {
	if reflect.ValueOf(value).Kind() != reflect.Ptr {
		return false, errors.New("value must be ptr")
	}

	var replay []byte

	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		replay, err = redis.Bytes(con.Do("GET", keyPatch(key)))

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
		err = Decode(replay, value)
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func (ru *RedisUtil) Del(ctx context.Context, key string) (err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		_, err = con.Do("DEL", keyPatch(key))

		return err
	})

	return err
}

func (ru *RedisUtil) Expire(ctx context.Context, key string, ttl int) (err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		_, err = con.Do("EXPIRE", keyPatch(key), ttl)

		return err
	})

	return err
}

func (ru *RedisUtil) TTL(ctx context.Context, key string) (ttl int, err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		ttl, err = redis.Int(con.Do("TTL", keyPatch(key)))

		return err
	})

	return ttl, err
}

func (ru *RedisUtil) Incr(ctx context.Context, key string) (res int64, err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		res, err = redis.Int64(con.Do("INCR", keyPatch(key)))

		return err
	})

	return res, err
}

func (ru *RedisUtil) IncrBy(ctx context.Context, key string, diff int64) (res int64, err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		res, err = redis.Int64(con.Do("INCRBY", keyPatch(key), diff))

		return err
	})

	return res, err
}

func (ru *RedisUtil) Decr(ctx context.Context, key string) (res int64, err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		res, err = redis.Int64(con.Do("DECR", keyPatch(key)))

		return err
	})

	return res, err
}

func (ru *RedisUtil) DecrBy(ctx context.Context, key string, diff int64) (res int64, err error) {
	err = ru.WrapDo(ctx, func(con redis.Conn) error {
		res, err = redis.Int64(con.Do("DECRBY", keyPatch(key), diff))

		return err
	})

	return res, err
}

func (ru *RedisUtil) WrapDo(ctx context.Context, doFunction func(con redis.Conn) error) error {
	con := ru.pool.Get()
	defer con.Close()

	return doFunction(con)
}
