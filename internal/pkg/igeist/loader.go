package igeist

import (
	"context"

	"github.com/zpiroux/geist/internal/pkg/model"
)

// Loader interface required for stream sink Loader implementations.
// Only certain types of Loader implementations might support multiple input Transformed object.
// For example, while a BigTable implementation might only support a single (first) Transformed object,
// as input to which fields from a single event to insert to the table, a Kafka implementation might
// receive multiple Transformed object (as the result from a EventSplit transformation type), all of
// which should be sent as separate events to the specified Kafka topic.
type Loader interface {

	// If successful the event/resource ID of the loaded event is returned.
	// If input 'data' is nil or empty, an error is to be returned.
	StreamLoad(ctx context.Context, data []*model.Transformed) (string, error, bool)

	// Called by Executor during shutdown of the stream
	Shutdown()
}
