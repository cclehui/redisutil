package test

import (
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strings"

	"gopkg.in/yaml.v3"
)

// 判断是否是go test 模式
func isGotest() bool {
	for _, val := range os.Args {
		if strings.HasPrefix(val, "-test") {
			return true
		}
	}

	return false
}

// go test 需要的 config init
//
//nolint:gochecknoinits
func init() {
	if !isGotest() {
		return
	}

	if Conf == nil {
		var abPath string

		_, filename, _, ok := runtime.Caller(0)

		if ok {
			abPath = path.Dir(filename)
		}

		Conf = DecodeUnitConfigFromLocal(fmt.Sprintf("%s/config.yaml", abPath))
	}
}

// 从本地解析单测配置文件
func DecodeUnitConfigFromLocal(configPath string) *UnitConfig {
	configFile, err := os.Open(configPath)
	if err != nil {
		panic(err)
	}
	defer configFile.Close()

	configData, err := io.ReadAll(configFile)
	if err != nil {
		panic(err)
	}

	cfg := new(UnitConfig)

	err = yaml.Unmarshal(configData, cfg)
	if err != nil {
		panic(err)
	}

	return cfg
}
