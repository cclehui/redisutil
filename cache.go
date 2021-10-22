package redisutil

import (
	"reflect"

	"github.com/pkg/errors"

	"github.com/gomodule/redigo/redis"
)

type CacheUtil struct {
	pool *redis.Pool
}

func NewCacheUtil(pool *redis.Pool) *CacheUtil {
	return &CacheUtil{pool}
}

func (cache *CacheUtil) Set(key string, value interface{}, ttl int) (err error) {
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

	err = cache.WrapDo(func(con redis.Conn) error {
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

func (cache *CacheUtil) Get(key string, value interface{}) (hit bool, err error) {
	if reflect.ValueOf(value).Kind() != reflect.Ptr {
		return false, errors.New("value must be ptr")
	}

	var replay []byte

	err = cache.WrapDo(func(con redis.Conn) error {
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

func (cache *CacheUtil) Del(key string) (err error) {
	err = cache.WrapDo(func(con redis.Conn) error {
		_, err = con.Do("DEL", keyPatch(key))

		return err
	})

	return err
}

func (cache *CacheUtil) Expire(key string, ttl int) (err error) {
	err = cache.WrapDo(func(con redis.Conn) error {
		_, err = con.Do("EXPIRE", keyPatch(key), ttl)

		return err
	})

	return err
}

func (cache *CacheUtil) TTL(key string) (ttl int, err error) {
	err = cache.WrapDo(func(con redis.Conn) error {
		ttl, err = redis.Int(con.Do("TTL", keyPatch(key)))

		return err
	})

	return ttl, err
}

func (cache *CacheUtil) Incr(key string) (res int64, err error) {
	err = cache.WrapDo(func(con redis.Conn) error {
		res, err = redis.Int64(con.Do("INCR", keyPatch(key)))

		return err
	})

	return res, err
}

func (cache *CacheUtil) IncrBy(key string, diff int64) (res int64, err error) {
	err = cache.WrapDo(func(con redis.Conn) error {
		res, err = redis.Int64(con.Do("INCRBY", keyPatch(key), diff))

		return err
	})

	return res, err
}

func (cache *CacheUtil) Decr(key string) (res int64, err error) {
	err = cache.WrapDo(func(con redis.Conn) error {
		res, err = redis.Int64(con.Do("DECR", keyPatch(key)))

		return err
	})

	return res, err
}

func (cache *CacheUtil) DecrBy(key string, diff int64) (res int64, err error) {
	err = cache.WrapDo(func(con redis.Conn) error {
		res, err = redis.Int64(con.Do("DECRBY", keyPatch(key), diff))

		return err
	})

	return res, err
}

func (cache *CacheUtil) WrapDo(doFunction func(con redis.Conn) error) error {
	con := cache.pool.Get()
	defer con.Close()

	return doFunction(con)
}
