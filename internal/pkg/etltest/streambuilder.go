package etltest

import (
	"context"

	"github.com/zpiroux/geist/internal/pkg/igeist"
)

type StreamBuilder struct {
	entityFactory igeist.StreamEntityFactory
}

func NewStreamBuilder(entityFactory igeist.StreamEntityFactory) *StreamBuilder {

	return &StreamBuilder{entityFactory: entityFactory}
}

func (s *StreamBuilder) Build(ctx context.Context, spec igeist.Spec) (igeist.Stream, error) {
	id := "mockId"
	extractor, err := s.entityFactory.CreateExtractor(ctx, spec, id)
	if err != nil {
		return nil, err
	}
	transformer, err := s.entityFactory.CreateTransformer(ctx, spec)
	if err != nil {
		return nil, err
	}
	loader, err := s.entityFactory.CreateLoader(ctx, spec, id)
	if err != nil {
		return nil, err
	}
	sinkExtractor, err := s.entityFactory.CreateSinkExtractor(ctx, spec, id)
	if err != nil {
		return nil, err
	}

	return NewStream(spec, extractor, transformer, loader, sinkExtractor), nil
}
