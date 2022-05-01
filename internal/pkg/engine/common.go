package engine

import (
	"context"
	"reflect"
	"time"
)

func isNil(v any) bool {
	return v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil())
}

// A context aware sleep func returning true if proper timeout after sleep and false if ctx canceled
func sleepCtx(ctx context.Context, delay time.Duration) bool {
	select {
	case <-time.After(delay):
		return true
	case <-ctx.Done():
		return false
	}
}
