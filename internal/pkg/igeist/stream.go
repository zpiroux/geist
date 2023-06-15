package igeist

import (
	"context"

	"github.com/zpiroux/geist/entity"
)

type Stream interface {
	Spec() *entity.Spec
	Instance() string
	Extractor() entity.Extractor
	Transformer() Transformer
	Loader() entity.Loader
	Publish(ctx context.Context, event []byte) (string, error)
	ExtractFromSink(
		ctx context.Context,
		query entity.ExtractorQuery,
		result *[]*entity.Transformed) (error, bool)
}
