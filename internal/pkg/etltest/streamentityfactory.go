package etltest

import (
	"context"
	"fmt"

	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/entity/transform"
	"github.com/zpiroux/geist/internal/pkg/entity/channel"
	"github.com/zpiroux/geist/internal/pkg/entity/void"
	"github.com/zpiroux/geist/internal/pkg/igeist"
)

// The following constants are only used here locally for test spec purposes,
// and there are no dependencies to the real plugin entities, like kafka, etc.
const (
	EntityKafka     = "kafka"
	EntityPubsub    = "pubsub"
	EntityFirestore = "firestore"
	EntityBigTable  = "bigtable"
	EntityBigQuery  = "bigquery"
)

type StreamEntityFactory struct {
	adminLoader entity.Loader
}

func NewStreamEntityFactory() *StreamEntityFactory {

	return &StreamEntityFactory{}
}

func (s *StreamEntityFactory) SetAdminLoader(loader entity.Loader) {
	s.adminLoader = loader
}

func (s *StreamEntityFactory) CreateExtractor(ctx context.Context, spec *entity.Spec, instanceId string) (entity.Extractor, error) {

	switch spec.Source.Type {

	case EntityKafka, EntityPubsub:
		return NewMockExtractor(spec.Source.Config), nil

	case entity.EntityGeistApi:
		// In real StreamEntityFactory the channel extractory factory is only created once
		return channel.NewExtractorFactory().NewExtractor(ctx, entity.Config{Spec: spec, ID: "instanceId"})

	default:
		return nil, fmt.Errorf("source type '%s' not implemented", spec.Source.Type)
	}
}

func (s *StreamEntityFactory) CreateSinkExtractor(ctx context.Context, spec *entity.Spec, instanceId string) (entity.Extractor, error) {

	switch spec.Sink.Type {

	case EntityFirestore:
		return NewMockExtractor(spec.Source.Config), nil

	case "void": // Used to support top-level api server testing
		return NewMockExtractor(spec.Source.Config), nil

	default:
		// This is not an error since sinks are not required to provide an extractor.
		// It's only the extractor for the source that is required.
		return nil, nil
	}
}

func (s *StreamEntityFactory) CreateTransformer(ctx context.Context, spec *entity.Spec) (igeist.Transformer, error) {
	// Currently only supporting native GEIST Transformations
	return transform.NewTransformer(spec), nil
}

func (s *StreamEntityFactory) CreateLoader(ctx context.Context, spec *entity.Spec, instanceId string) (entity.Loader, error) {

	switch spec.Sink.Type {

	case
		EntityBigTable,
		EntityFirestore,
		EntityKafka,
		EntityBigQuery:
		return NewMockLoader(), nil

	case entity.EntityVoid:
		return void.NewLoaderFactory().NewLoader(ctx, entity.Config{Spec: spec, ID: instanceId})

	case entity.EntityAdmin:
		return s.adminLoader, nil

	default:
		return nil, fmt.Errorf("sink type '%s' not implemented", spec.Sink.Type)
	}
}
