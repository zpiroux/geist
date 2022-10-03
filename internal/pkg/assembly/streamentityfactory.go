package assembly

import (
	"context"
	"fmt"

	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/entity/transform"
	"github.com/zpiroux/geist/internal/pkg/igeist"
)

// StreamEntityFactory creates stream entities based on Stream Spec config and is a singleton,
// created by the Service, and operated by the StreamBuilder (also a singleton), which is given
// to the Supervisor during creation.
type StreamEntityFactory struct {
	config      Config
	adminLoader entity.Loader
}

func NewStreamEntityFactory(config Config) *StreamEntityFactory {
	return &StreamEntityFactory{config: config}
}

func (s *StreamEntityFactory) SetAdminLoader(loader entity.Loader) {
	// TODO: With the addition of loader factories in config, the admin loader could be moved there
	s.adminLoader = loader
}

func (s *StreamEntityFactory) CreateExtractor(ctx context.Context, etlSpec igeist.Spec, instanceId string) (entity.Extractor, error) {

	c := entity.Config{
		Spec:       etlSpec.(*entity.Spec),
		ID:         instanceId,
		NotifyChan: s.config.NotifyChan,
		Log:        s.config.Log,
	}

	if factory, ok := s.config.Extractors[string(c.Spec.Source.Type)]; ok {
		return factory.NewExtractor(ctx, c)
	}
	return nil, fmt.Errorf("no extractor factory found for source type %s, in spec: %+v", c.Spec.Source.Type, c.Spec)

}

// CreateSinkExtractor creates an extractor belonging to a specific sink loader, enabling reading data that the loader has
// written to the sink.
func (s *StreamEntityFactory) CreateSinkExtractor(ctx context.Context, etlSpec igeist.Spec, instanceId string) (entity.Extractor, error) {

	c := entity.Config{
		Spec:       etlSpec.(*entity.Spec),
		ID:         instanceId,
		NotifyChan: s.config.NotifyChan,
		Log:        s.config.Log,
	}

	// If we have a loader defined for this sink/loader type, we should create a sink extractor if it's supported by the loader
	if factory, ok := s.config.Loaders[string(c.Spec.Sink.Type)]; ok {
		return factory.NewSinkExtractor(ctx, c)
	}

	// This is not an error since *sinks* are not required to provide an extractor.
	// It's only the extractor for the *source* that is required.
	return nil, nil
}

func (s *StreamEntityFactory) CreateTransformer(ctx context.Context, etlSpec igeist.Spec) (igeist.Transformer, error) {

	// Currently only supporting native GEIST Transformations
	spec := etlSpec.(*entity.Spec)
	switch spec.Transform.ImplId {
	default:
		return transform.NewTransformer(spec), nil
	}
}

func (s *StreamEntityFactory) CreateLoader(ctx context.Context, etlSpec igeist.Spec, instanceId string) (entity.Loader, error) {

	c := entity.Config{
		Spec:       etlSpec.(*entity.Spec),
		ID:         instanceId,
		NotifyChan: s.config.NotifyChan,
		Log:        s.config.Log,
	}

	if factory, ok := s.config.Loaders[string(c.Spec.Sink.Type)]; ok {
		return factory.NewLoader(ctx, c)
	}

	// TODO: This is a left-over from before loader factories.
	// Add this sink type to default native ones in s.config.Loaders.
	if c.Spec.Sink.Type == entity.EntityAdmin {
		return s.adminLoader, nil
	}

	return nil, fmt.Errorf("no loader factory found for sink type %s, in spec: %+v", c.Spec.Sink.Type, c.Spec)
}

func (s *StreamEntityFactory) Entities() map[string]map[string]bool {

	e := make(map[string]map[string]bool)
	e["extractor"] = make(map[string]bool)
	e["loader"] = make(map[string]bool)
	for id := range s.config.Extractors {
		e["extractor"][id] = true
	}
	for id := range s.config.Loaders {
		e["loader"][id] = true
	}
	return e
}
