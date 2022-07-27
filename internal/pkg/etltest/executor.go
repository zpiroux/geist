package etltest

import (
	"context"
	"sync"

	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/internal/pkg/igeist"
)

type MockExecutor struct {
	stream igeist.Stream
}

func NewExecutor(stream igeist.Stream) *MockExecutor {

	return &MockExecutor{stream: stream}
}

func (e *MockExecutor) StreamId() string {
	return e.stream.Spec().Id()
}

func (e *MockExecutor) Spec() igeist.Spec {
	return e.stream.Spec()
}

func (e *MockExecutor) Run(ctx context.Context, wg *sync.WaitGroup) {
	var (
		err       error
		retryable bool
	)

	defer wg.Done()
	e.stream.Extractor().StreamExtract(ctx, e.ProcessEvent, &err, &retryable)

}

func (e *MockExecutor) ProcessEvent(ctx context.Context, events []entity.Event) entity.EventProcessingResult {

	var (
		r           entity.EventProcessingResult
		transformed []*entity.Transformed
	)

	r.Retryable = true

	for _, event := range events {

		var transEvent []*entity.Transformed
		transEvent, r.Error = e.stream.Transformer().Transform(ctx, event.Data, &r.Retryable)
		if r.Error != nil {
			return r
		}
		transformed = append(transformed, transEvent...)
	}

	if transformed != nil {
		r.ResourceId, r.Error, r.Retryable = e.stream.Loader().StreamLoad(ctx, transformed)
	}

	return r
}

func (e *MockExecutor) Stream() igeist.Stream {
	return e.stream
}

func (e *MockExecutor) Shutdown() {}
