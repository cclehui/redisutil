package redisutil

import (
	"reflect"
	"strconv"

	"github.com/pkg/errors"
)

var ErrInvalidNum = errors.New("Invalid num")

// 为了以后升级可以统一让所有key失效
func keyPatch(key string) string {
	return key
}

func isNum(i interface{}) (string, bool) {
	switch vi := i.(type) {
	case int8:
		v := int64(vi)
		s := strconv.FormatInt(v, 10)

		return s, true
	case int16:
		v := int64(vi)
		s := strconv.FormatInt(v, 10)

		return s, true
	case int32:
		v := int64(vi)
		s := strconv.FormatInt(v, 10)

		return s, true
	case int:
		v := int64(vi)
		s := strconv.FormatInt(v, 10)

		return s, true
	case int64:
		s := strconv.FormatInt(vi, 10)

		return s, true

	case uint8:
		v := uint64(vi)
		s := strconv.FormatUint(v, 10)

		return s, true
	case uint16:
		v := uint64(vi)
		s := strconv.FormatUint(v, 10)

		return s, true
	case uint32:
		v := uint64(vi)
		s := strconv.FormatUint(v, 10)

		return s, true
	case uint:
		v := uint64(vi)
		s := strconv.FormatUint(v, 10)

		return s, true
	case uint64:
		s := strconv.FormatUint(vi, 10)

		return s, true

	default:
		return "", false
	}
}

func isNumPtr(i interface{}) bool {
	val := reflect.ValueOf(i)
	if val.Kind() != reflect.Ptr {
		return false
	}

	v := val.Elem().Kind()

	switch v {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int, reflect.Int64:
		return true
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint, reflect.Uint64:
		return true
	default:
		return false
	}
}

// 注：此方法溢出不会报error
func bytesToNum(b []byte, num interface{}) error {
	val := reflect.ValueOf(num)
	if val.Kind() != reflect.Ptr {
		return ErrInvalidNum
	}

	v := val.Elem().Kind()
	s := string(b)

	switch v {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int, reflect.Int64:
		n, err := strconv.ParseInt(s, 0, 64)
		if err != nil {
			return errors.WithMessagef(err, "%s convert to int err", s)
		}

		val.Elem().SetInt(n)
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint, reflect.Uint64:
		n, err := strconv.ParseUint(s, 0, 64)
		if err != nil {
			return errors.WithMessagef(err, "%s convert to uint err", s)
		}

		val.Elem().SetUint(n)
	}

	return nil
}
