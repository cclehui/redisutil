package test

var Conf *UnitConfig

type EndpointConfig struct {
	Address string `yaml:"address"`
	Port    int    `yaml:"port"`
}

type UnitConfig struct {
	Redis *struct {
		// 数据库名
		DB int `yaml:"db"`
		// 连接地址
		Endpoint *EndpointConfig `yaml:"endpoint"`
		// 校验密码
		Auth string `yaml:"auth"`
	} `yaml:"redis"`
}
