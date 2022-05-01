package igeist

import (
	"context"
)

type StreamEntityFactory interface {
	SetAdminLoader(loader Loader)
	CreateExtractor(ctx context.Context, spec Spec, instanceId string) (Extractor, error)
	CreateSinkExtractor(ctx context.Context, spec Spec, instanceId string) (Extractor, error)
	CreateTransformer(ctx context.Context, spec Spec) (Transformer, error)
	CreateLoader(ctx context.Context, spec Spec, instanceId string) (Loader, error)
}
