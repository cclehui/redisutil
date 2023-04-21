package base

import (
	"bytes"
	"encoding/gob"
	"reflect"

	"github.com/pkg/errors"
)

// Encode将任意类型编码为[]byte类型
func Encode(val interface{}) ([]byte, error) {
	var b bytes.Buffer
	encoder := gob.NewEncoder(&b)

	err := encoder.Encode(val)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return b.Bytes(), nil
}

// Decode将Encode的结果重新赋值给ptr指向的类型
func Decode(b []byte, ptr interface{}) error {
	decoder := gob.NewDecoder(bytes.NewReader(b))

	err := decoder.Decode(ptr)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func DecodeReflectValue(b []byte, value reflect.Value) error {
	decoder := gob.NewDecoder(bytes.NewReader(b))

	err := decoder.DecodeValue(value)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
