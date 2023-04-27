package redisutil

type Option interface {
	Apply(*RedisUtil)
}

type OptionFunc func(cacheUtil *RedisUtil)

func (of OptionFunc) Apply(cacheUtil *RedisUtil) {
	of(cacheUtil)
}

func OptionSingleFlightGroupNum(num int) Option {
	return OptionFunc(func(cacheUtil *RedisUtil) {
		cacheUtil.singleFlightGroupNum = num
	})
}

func OptionLogger(logger Logger) Option {
	return OptionFunc(func(cacheUtil *RedisUtil) {
		cacheUtil.logger = logger
	})
}
