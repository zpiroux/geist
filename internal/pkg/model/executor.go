package model

import (
	"context"
	"fmt"
	"time"
)

type Event struct {
	Data []byte
	Ts   time.Time
	Key  []byte
}

func (e Event) String() string {
	return fmt.Sprintf("key: %s, ts: %v, data: %s\n", string(e.Key), e.Ts, string(e.Data))
}

type ProcessEventFunc func(context.Context, []Event) EventProcessingResult

type ExecutorStatus int

const (
	ExecutorStatusInvalid ExecutorStatus = iota
	ExecutorStatusSuccessful
	ExecutorStatusError
	ExecutorStatusRetriesExhausted
	ExecutorStatusShutdown
)

type EventProcessingResult struct {
	Status     ExecutorStatus
	ResourceId string
	Error      error
	Retryable  bool
}
