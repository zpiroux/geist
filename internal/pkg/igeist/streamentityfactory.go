package igeist

import (
	"context"

	"github.com/zpiroux/geist/entity"
)

type StreamEntityFactory interface {
	SetAdminLoader(loader entity.Loader)
	CreateExtractor(ctx context.Context, spec Spec, instanceId string) (entity.Extractor, error)
	CreateSinkExtractor(ctx context.Context, spec Spec, instanceId string) (entity.Extractor, error)
	CreateTransformer(ctx context.Context, spec Spec) (Transformer, error)
	CreateLoader(ctx context.Context, spec Spec, instanceId string) (entity.Loader, error)
}
