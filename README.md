# 功能特性
基于redigo 的redis操作封装 和缓存cache wrapper
1. set, get, mget, batchset 
2. 缓存操作封装 CacheWrapper
3. 缓存批量操作封装 CacheWrapperMget
4. 缓存支持singleflight , 支持flush

# gotest 启动方法
redis_util_test.go 和redis_util_cache_test.go 包含了go test 运行demo

1. 配置 internal/test/config.yaml  中的redis配置信息
2. go test -v

# 使用方法

### 基础使用

```
	redisUtil := redisutil.NewRedisUtil(redisPool)
	cacheKey := "cclehui_test_set_get_key_211022"

	_ = redisUtil.Set(ctx, cacheKey, "axxxaa", 3600) // 设置缓存

	valueStrRes := ""
	_, _ = redisUtil.Get(ctx, cacheKey, &valueStrRes) // 获取缓存
	fmt.Println("获取缓存:", valueStrRes)

```

### 缓存wrapper
redis_util_cache_test.go 中有更多的使用例子

```
	redisUtil := NewRedisUtil(getTestPool())
	key := "gotest:redis_wrapper:get"

	type valueStruct struct {
		Name string
		Age  int
	}

	fallbackFunc := func() (interface{}, error) {
		fmt.Printf( "fallback 缓存穿透 call real function\n")
		time.Sleep(time.Second * 1)

		data := &valueStruct{
			Name: "xxxxxx",
			Age:  18,
		}

		return data, nil
	}

	resultData := &valueStruct{}

	paramsTemp := &WrapperParams{
		Key:           Key,
		ExpireSeconds: 600,
		FallbackFunc:  fallbackFunc,
		Result:        resultData,
	}

	paramsTemp.Result = &dataResult[j]

    redisUtil.CacheWrapper(ctx, paramsTemp)
```

