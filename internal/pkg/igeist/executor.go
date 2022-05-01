package igeist

import (
	"context"
	"sync"

	"github.com/zpiroux/geist/internal/pkg/model"
)

// Executor interface required for Stream ETL Executors
type Executor interface {
	Stream() Stream
	StreamId() string
	Run(ctx context.Context, wg *sync.WaitGroup)

	// ProcessEvent returns the processed/persisted event's event/resource ID, together with error, retryable.
	// The implementation varies across different sink types, but the returned resource ID it will be the key
	// used for key lookups of the event (e.g. row-key for BigTable), and will be used as the resource ID when
	// using API as Source.
	ProcessEvent(ctx context.Context, events []model.Event) model.EventProcessingResult
	Shutdown()
}
