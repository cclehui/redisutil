package redisutil

import (
	"context"
	"encoding/json"
	"fmt"
)

const (
	LevelDebug = "DEBUG"
	LevelInfo  = "INFO"
	LevelWarn  = "WARN"
	LevelError = "ERROR"
)

type LogData struct {
	Level   string
	Content string
}

type Logger interface {
	Errorf(ctx context.Context, format string, args ...interface{})
	Infof(ctx context.Context, format string, args ...interface{})
	Printf(format string, args ...interface{})
}

var logger Logger = &DefaultLogger{}

func GetLogger() Logger {
	return logger
}

func SetLogger(newLogger Logger) {
	logger = newLogger
}

type DefaultLogger struct{}

func (l *DefaultLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	content := fmt.Sprintf(format, args...)
	logStr, _ := json.Marshal(LogData{Level: LevelError, Content: content})
	fmt.Println(string(logStr))
}

func (l *DefaultLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	content := fmt.Sprintf(format, args...)
	logStr, _ := json.Marshal(LogData{Level: LevelInfo, Content: content})
	fmt.Println(string(logStr))
}

func (l *DefaultLogger) Printf(format string, args ...interface{}) {
	content := fmt.Sprintf(format, args...)
	logStr, _ := json.Marshal(LogData{Level: LevelInfo, Content: content})
	fmt.Println(string(logStr))
}
