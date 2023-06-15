package igeist

import (
	"context"

	"github.com/zpiroux/geist/entity"
)

type StreamBuilder interface {
	Build(ctx context.Context, spec *entity.Spec) (Stream, error)
}
