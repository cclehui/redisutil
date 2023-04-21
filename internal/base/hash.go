package base

import (
	"encoding/json"
	"hash/crc32"
)

func CRC32(params ...interface{}) uint32 {
	dataBytes, _ := json.Marshal(params)

	return crc32.ChecksumIEEE(dataBytes)
}
