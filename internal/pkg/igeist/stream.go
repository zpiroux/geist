package igeist

import (
	"context"

	"github.com/zpiroux/geist/internal/pkg/model"
)

type Stream interface {
	Spec() Spec
	Instance() string
	Extractor() Extractor
	Transformer() Transformer
	Loader() Loader
	Publish(ctx context.Context, event []byte) (string, error)
	ExtractFromSink(
		ctx context.Context,
		query model.ExtractorQuery,
		result *[]*model.Transformed) (error, bool)
}
