package igeist

import (
	"context"

	"github.com/zpiroux/geist/entity"
)

// Transformer interface required for transformer implementations
type Transformer interface {
	// Based on input event data, and the transformation rules in the Spec, the Transform() function returns
	// a key-value map where keys are "id" fields from Transform spec, and values are the transformation results.
	// The output from Transform() can contain multiple new events, in case of applied
	// event-split transformations.
	// If Transform() succeeded, but the transformation resulted in no output, e.g. for non-applicable incoming events,
	// the return values are nil, nil (i.e., not regarded as an error).
	Transform(
		ctx context.Context,
		event []byte,
		retryable *bool) ([]*entity.Transformed, error)
}
