package redisutil

type Option interface {
	Apply(*CacheUtil)
}

type OptionFunc func(cacheUtil *CacheUtil)

func (of OptionFunc) Apply(cacheUtil *CacheUtil) {
	of(cacheUtil)
}

func OptionSingleFlightGroupNum(num int) Option {
	return OptionFunc(func(cacheUtil *CacheUtil) {
		cacheUtil.singleFlightGroupNum = num
	})
}
