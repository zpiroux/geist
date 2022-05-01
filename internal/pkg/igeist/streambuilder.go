package igeist

import (
	"context"
)

type StreamBuilder interface {
	Build(ctx context.Context, spec Spec) (Stream, error)
}
