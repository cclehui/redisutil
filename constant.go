package redisutil

const (
	DefaultSingleFlightGroupNum = 10

	TTLNoExpire = -1 // 不过期
)

type FallbackFunc func() (interface{}, error)

type MgetFallbackFunc func(fallbackIndex int) (interface{}, error)

type MgetBatchFallbackFunc func(fallbackIndexes []int) (map[int]interface{}, error)
