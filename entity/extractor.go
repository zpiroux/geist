package entity

import (
	"context"
	"fmt"
	"time"
)

type ExtractorFactories map[string]ExtractorFactory

// ExtractorFactory enables loaders/sinks to be handled as plug-ins to Geist.
// A factory is registered with Geist API RegisterLoaderType() for a source
// type to be available for stream specs.
type ExtractorFactory interface {
	// SourceId returns the source ID for which the extractor is implemented
	SourceId() string

	// NewExtractor creates a new extractor entity
	NewExtractor(ctx context.Context, c Config) (Extractor, error)

	// Close is called by Geist after client has called Geist API geist.Shutdown()
	Close() error
}

// Extractor is the interface required for stream source extractor implementations
// and for sink queries. The Extractor implementation should be given its GEIST Spec
// in a constructor.
//
// For source stream extractors the only function required to be fully functional is
// StreamExtract().
//
// For sink extractors the only function required to be fully functional is ExtractFromSink().
//
// The others are situational depending on extractor/source entity type, and could be
// empty, e.g. simply returning nil, false (or someerror, false).
type Extractor interface {

	// StreamExtract (required) continuously consumes events from its source (until ctx is canceled),
	// and report each consumed event back to Executor with reportEvent(), for further processing.
	StreamExtract(
		ctx context.Context,
		reportEvent ProcessEventFunc,
		err *error,
		retryable *bool)

	// Extract (optional) provides generic extraction from the source based on the provided query,
	// and returns directly.
	Extract(ctx context.Context, query ExtractorQuery, result any) (error, bool)

	// ExtractFromSink (optional) extracts data from the sink used in an ETL Stream, as specified
	// in the Extractors GEIST spec. Currently only supported by Firestore and BigTable extractors.
	ExtractFromSink(ctx context.Context, query ExtractorQuery, result *[]*Transformed) (error, bool)

	// SendToSource (optional) enables external clients to send events directly to the Extractor's
	// Source with Geist.Publish().
	// For source connectors meant to be used in admin streams, this method is required.
	// Currently known connectors that implement this method are:
	//		* "geistapi" (channel) extractor
	// 		* "pubsub" GCP extractor
	//		* "kafka" extractor
	SendToSource(ctx context.Context, event any) (string, error)
}

//
// Types, etc for communication between an Extractor and it's Executor (handled internally by Geist)
//
// Only externally needed for implementations of new source/sink extractors/loaders.
//

// ProcessEventFunc is the type of func that an Extractor calls for each extracted event to be processed
// downstream.
//
// It is important for the Extractor to properly handle the returned EventProcessingResult.
//
//	EventProcessingResult.ExecutorStatus values:
//		ExecutorStatusSuccessful --> continue as normal
//		ExecutorStatusError --> handle error depending on Houe mode in stream spec
//		ExecutorStatusRetriesExhausted --> normally a shutdown of extractor is an ok action (will be restarted)
//		ExecutorStatusShutdown --> shut down extractor
//
type ProcessEventFunc func(context.Context, []Event) EventProcessingResult

type Event struct {
	Data []byte
	Ts   time.Time
	Key  []byte
}

func (e Event) String() string {
	return fmt.Sprintf("key: %s, ts: %v, data: %s\n", string(e.Key), e.Ts, string(e.Data))
}

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

//
// Below is needed for Extractors providing query posibilities from sinks, e.g. getting an inserted
// value from a key.
//

type QueryType int

const (
	Unknown QueryType = iota
	QueryTypeKeyValue
	QueryTypeCompositeKeyValue
	QueryTypeAll
)

type ExtractorQuery struct {
	Type         QueryType
	Key          string
	CompositeKey []KeyValueFilter
}

type KeyValueFilter struct {
	Key   string
	Value string
}
