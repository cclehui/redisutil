package goroutine

import (
	"sync"

	"github.com/cclehui/redisutil/log"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
)

var goRoutineSize = 500

var goRoutineOptions = []ants.Option{ants.WithLogger(log.GetLogger())}

var goPool *ants.Pool
var goPoolOnce = sync.Once{}

func getGoPool() (result *ants.Pool, err error) {
	goPoolOnce.Do(func() {
		goPool, err = ants.NewPool(goRoutineSize, goRoutineOptions...)
	})

	result = goPool

	if err != nil {
		err = errors.WithStack(err)
	}

	return result, err
}

func SetGoRoutineSize(size int) {
	goRoutineSize = size
}

func SetGoRoutineOptions(options []ants.Option) {
	goRoutineOptions = options
}

type ErrorGroup struct {
}
