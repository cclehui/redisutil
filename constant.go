package redisutil

const (
	DefaultSingleFlightGroupNum = 10

	TTLNoExpire = -1 // 不过期
)

type CacheSetFunc func() (interface{}, error)

type MgetSetFunc func(fallbackIndex int) (interface{}, error)

type MgetBatchSetFunc func(fallbackIndexes []int) (map[int]interface{}, error)
