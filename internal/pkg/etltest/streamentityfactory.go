package etltest

import (
	"context"
	"errors"
	"fmt"

	"github.com/zpiroux/geist/internal/pkg/entity/channel"
	"github.com/zpiroux/geist/internal/pkg/entity/transform"
	"github.com/zpiroux/geist/internal/pkg/entity/void"
	"github.com/zpiroux/geist/internal/pkg/igeist"
	"github.com/zpiroux/geist/internal/pkg/model"
)

// TODO: Improve this Mock entity factory to create more granular extractors, etc, using
// the real Kafka/Pubsub, etc implementations, but mocked-out

type StreamEntityFactory struct {
	adminLoader igeist.Loader
}

func NewStreamEntityFactory() *StreamEntityFactory {

	return &StreamEntityFactory{}
}

func (s *StreamEntityFactory) SetAdminLoader(loader igeist.Loader) {
	s.adminLoader = loader
}

func (s *StreamEntityFactory) CreateExtractor(ctx context.Context, etlSpec igeist.Spec, instanceId string) (igeist.Extractor, error) {

	spec := etlSpec.(*model.Spec)

	switch spec.Source.Type {

	case model.EntityKafka:
		if spec.Source.Config.Topics[0].Env == "" {
			return nil, errors.New("invalid topic config, no environment defined")
		}
		return NewMockExtractor(spec.Source.Config), nil

	case model.EntityPubsub:
		return NewMockExtractor(spec.Source.Config), nil

	case model.EntityGeistApi:
		return channel.NewExtractor(spec.Id(), "instanceId")

	default:
		return nil, fmt.Errorf("source type '%s' not implemented", spec.Source.Type)
	}
}

func (s *StreamEntityFactory) CreateSinkExtractor(ctx context.Context, etlSpec igeist.Spec, instanceId string) (igeist.Extractor, error) {

	spec := etlSpec.(*model.Spec)
	switch spec.Sink.Type {

	case model.EntityFirestore:
		return NewMockExtractor(spec.Source.Config), nil

	case "void": // Used to support top-level api server testing
		return NewMockExtractor(spec.Source.Config), nil

	default:
		// This is not an error since sinks are not required to provide an extractor.
		// It's only the extractor for the source that is required.
		return nil, nil
	}
}

func (s *StreamEntityFactory) CreateTransformer(ctx context.Context, etlSpec igeist.Spec) (igeist.Transformer, error) {

	// Currently only supporting native GEIST Transformations
	spec := etlSpec.(*model.Spec)
	switch spec.Transform.ImplId {
	default:
		return transform.NewTransformer(spec), nil
	}
}

func (s *StreamEntityFactory) CreateLoader(ctx context.Context, etlSpec igeist.Spec, instanceId string) (igeist.Loader, error) {

	spec := etlSpec.(*model.Spec)
	switch spec.Sink.Type {

	case
		model.EntityBigTable,
		model.EntityFirestore,
		model.EntityKafka,
		model.EntityBigQuery:
		return NewMockLoader(), nil

	case model.EntityVoid:
		return void.NewLoader(spec)

	case model.EntityAdmin:
		return s.adminLoader, nil

	default:
		return nil, fmt.Errorf("sink type '%s' not implemented", spec.Sink.Type)
	}
}
