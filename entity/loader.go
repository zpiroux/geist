package entity

import (
	"context"
)

type LoaderFactories map[string]LoaderFactory

// LoaderFactory enables loaders/sinks to be handled as plug-ins to Geist.
// A factory is registered with Geist API RegisterLoaderType() for a sink
// type to be available for stream specs.
type LoaderFactory interface {
	// Sink returns the sink ID for which the loader is implemented
	SinkId() string

	// NewLoader creates a new loader entity
	NewLoader(ctx context.Context, c Config) (Loader, error)

	// NewSinkExtractor creates an extractor to enable retrieving data from the sink
	// as written by the loader.
	// This functionality is optional and if not implemented the function should return nil, nil.
	NewSinkExtractor(ctx context.Context, c Config) (Extractor, error)

	// Close is called by Geist after using Geist API geist.Shutdown()
	Close() error
}

// Loader interface required for stream sink Loader implementations.
// Only certain types of Loader implementations might support multiple input Transformed object.
// For example, while a BigTable implementation might only support a single (first) Transformed object,
// as input to which fields from a single event to insert to the table, a Kafka implementation might
// receive multiple Transformed object (as the result from a EventSplit transformation type), all of
// which should be sent as separate events to the specified Kafka topic.
type Loader interface {

	// If successful the event/resource ID of the loaded event is returned.
	// If input 'data' is nil or empty, an error is to be returned.
	StreamLoad(ctx context.Context, data []*Transformed) (string, error, bool)

	// Called by Executor during shutdown of the stream
	Shutdown()
}
