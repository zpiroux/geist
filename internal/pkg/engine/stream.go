package engine

import (
	"context"
	"errors"

	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/internal/pkg/igeist"
)

type Stream struct {
	spec          *entity.Spec
	extractor     entity.Extractor
	transformer   igeist.Transformer
	loader        entity.Loader
	sinkExtractor entity.Extractor
	instance      string
}

func NewStream(
	spec *entity.Spec,
	instance string,
	extractor entity.Extractor,
	transformer igeist.Transformer,
	loader entity.Loader,
	sinkExtractor entity.Extractor) *Stream {

	return &Stream{
		spec:          spec,
		instance:      instance,
		extractor:     extractor,
		transformer:   transformer,
		loader:        loader,
		sinkExtractor: sinkExtractor,
	}
}

func (s *Stream) Spec() *entity.Spec {
	return s.spec
}

func (s *Stream) Instance() string {
	return s.instance
}

func (s *Stream) Extractor() entity.Extractor {
	return s.extractor
}

func (s *Stream) Transformer() igeist.Transformer {
	return s.transformer
}

func (s *Stream) Loader() entity.Loader {
	return s.loader
}

// Publish makes it possible for Stream clients to send events directly to the source of the stream
func (s *Stream) Publish(ctx context.Context, event []byte) (string, error) {
	return s.extractor.SendToSource(ctx, event)
}

// ExtractFromSink fetches data stored in the Stream's sink. It uses a sink extractor for that purpose.
func (s *Stream) ExtractFromSink(ctx context.Context, query entity.ExtractorQuery, result *[]*entity.Transformed) (error, bool) {
	if s.sinkExtractor == nil {
		return errors.New("no sink extractor available for this sink"), false
	}
	return s.sinkExtractor.ExtractFromSink(ctx, query, result)
}
