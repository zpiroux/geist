package igeist

import (
	"context"

	"github.com/zpiroux/geist/internal/pkg/model"
)

// Extractor interface required for stream source extractor implementations and for sink queries.
// The Extractor implementation should be given its GEIST Spec in a constructor.
//
// For source stream extractors the only function required to be fully functional is StreamExtract().
//
// For sink extractors the only function required to be fully functional is ExtractFromSink().
//
// The others are situational depending on extractor/source entity type, and could be empty,
// e.g. simply returning nil, false (or someerror, false).
type Extractor interface {

	// StreamExtract continuously consumes events from its source (until ctx is canceled),
	// and report each consumed event back to Executor with reportEvent(), for further processing.
	StreamExtract(
		ctx context.Context,
		reportEvent model.ProcessEventFunc,
		err *error,
		retryable *bool)

	// Extract provides generic extraction from the source based on the provided query, and returns directly.
	// Optional functionality. Currently only supported by Firestore extractor (but not in use). Kept for future functionality.
	Extract(ctx context.Context, query model.ExtractorQuery, result any) (error, bool)

	// ExtractFromSink extracts data from the sink used in an ETL Stream, as specified in the Extractors GEIST spec.
	// Currently only supported by Firestore and BigTable extractors.
	ExtractFromSink(ctx context.Context, query model.ExtractorQuery, result *[]*model.Transformed) (error, bool)

	// SendToSource enables external clients to send events directly to the Extractor's Source.
	// Optional functionality. Currently only supported by "geistapi" (channel) extractor (for Publish() functionality)
	// and by Pubsub extractor (used in admin stream for posting registry change events to concurrent supervisors).
	SendToSource(ctx context.Context, event any) (string, error)
}
