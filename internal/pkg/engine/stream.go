package engine

import (
	"context"
	"errors"

	"github.com/zpiroux/geist/internal/pkg/igeist"
	"github.com/zpiroux/geist/internal/pkg/model"
)

type Stream struct {
	spec          igeist.Spec
	extractor     igeist.Extractor
	transformer   igeist.Transformer
	loader        igeist.Loader
	sinkExtractor igeist.Extractor
	instance      string
}

func NewStream(
	spec igeist.Spec,
	instance string,
	extractor igeist.Extractor,
	transformer igeist.Transformer,
	loader igeist.Loader,
	sinkExtractor igeist.Extractor) *Stream {

	return &Stream{
		spec:          spec,
		instance:      instance,
		extractor:     extractor,
		transformer:   transformer,
		loader:        loader,
		sinkExtractor: sinkExtractor,
	}
}

func (s *Stream) Spec() igeist.Spec {
	return s.spec
}

func (s *Stream) Instance() string {
	return s.instance
}

func (s *Stream) Extractor() igeist.Extractor {
	return s.extractor
}

func (s *Stream) Transformer() igeist.Transformer {
	return s.transformer
}

func (s *Stream) Loader() igeist.Loader {
	return s.loader
}

// Publish makes it possible for Stream clients to send events directly to the source of the stream
func (s *Stream) Publish(ctx context.Context, event []byte) (string, error) {
	return s.extractor.SendToSource(ctx, event)
}

// ExtractFromSink fetches data stored in the Stream's sink. It uses a sink extractor for that purpose.
func (s *Stream) ExtractFromSink(ctx context.Context, query model.ExtractorQuery, result *[]*model.Transformed) (error, bool) {
	if s.sinkExtractor == nil {
		return errors.New("no sink extractor available for this sink"), false
	}
	return s.sinkExtractor.ExtractFromSink(ctx, query, result)
}
