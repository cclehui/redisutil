//nolint:unparam
package redisutil

import (
	"context"

	"github.com/gomodule/redigo/redis"
)

func conDo(ctx context.Context, con redis.Conn, commandName string, args ...interface{}) (interface{}, error) {
	return con.Do(commandName, args...)
}

func conSend(ctx context.Context, con redis.Conn, commandName string, args ...interface{}) error {
	return con.Send(commandName, args...)
}

func conFlush(ctx context.Context, con redis.Conn) error {
	return con.Flush()
}

func conReceive(ctx context.Context, con redis.Conn) (interface{}, error) {
	return con.Receive()
}
