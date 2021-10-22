package redisutil

import (
	"bytes"
	"encoding/gob"
)

// Encode将任意类型编码为[]byte类型
func Encode(val interface{}) ([]byte, error) {
	var b bytes.Buffer
	encoder := gob.NewEncoder(&b)

	err := encoder.Encode(val)
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

// Decode将Encode的结果重新赋值给ptr指向的类型
func Decode(b []byte, ptr interface{}) error {
	decoder := gob.NewDecoder(bytes.NewReader(b))

	err := decoder.Decode(ptr)
	if err != nil {
		return err
	}

	return nil
}
