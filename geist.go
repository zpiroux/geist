package geist

import (
	"context"
	"errors"

	"github.com/zpiroux/geist/internal/pkg/model"
	"github.com/zpiroux/geist/internal/service"
)

const (
	ErrGeistNotInitialized    = "geist not initialized"
	ErrSpecAlreadyExists      = "stream ID already exists with that version - increment version number to upgrade"
	ErrInvalidStreamSpec      = "stream Specification is not valid"
	ErrInvalidStreamId        = "invalid Stream ID"
	ErrProtectedStreamId      = "spec format is valid but but stream ID is protected and cannot be used"
	ErrCodeInvalidSpecRegOp   = "use RegisterStream() instead to register new streams"
	ErrInternalDataProcessing = "internal data processing error"
)

type StreamSpec struct {
	model.Spec
}

type Geist struct {
	service *service.Service
	cancel  context.CancelFunc
}

// New creates and configures Geist's internal services and all previously registered streams,
// based on the provided config.
func New(ctx context.Context, config Config) (g *Geist, err error) {
	g = &Geist{}
	g.service, err = service.New(ctx, toInternalConfig(config))
	return g, err
}

// Run starts up Geist's internal services and all previously registered streams
// (if Geist configured to use persistent registry), as prepared by New().
// It is a blocking call until Geist is shut down, from a call to Shutdown.
// Geist must be up and running prior to usage of the other Geist API functions.
func (g *Geist) Run(ctx context.Context) (err error) {
	if g.service == nil {
		return errors.New(ErrGeistNotInitialized)
	}

	ctx, g.cancel = context.WithCancel(ctx)
	return g.service.Run(ctx)
}

// RegisterStream validates and persist the stream spec in the chosen registry implementation.
// If successful, the registered stream is started up immediately, and the generated ID of the stream
// is returned.
func (g *Geist) RegisterStream(ctx context.Context, specData []byte) (id string, err error) {

	registry := g.service.Registry()
	_, err = registry.Validate(specData)
	if err != nil {
		return id, errors.New(ErrInvalidStreamSpec + ", details: " + err.Error())
	}

	exists, err := registry.ExistsSameVersion(specData)

	if err != nil {
		return id, errors.New(ErrInternalDataProcessing + ", details: " + err.Error())
	}
	if exists {
		return id, errors.New(ErrSpecAlreadyExists)
	}

	id, err = registry.Stream().Publish(ctx, specData)

	if err != nil {
		return id, errors.New(ErrInternalDataProcessing + ", details: " + err.Error())
	}
	return
}

// Publish sends the provided event to the source extractor of the stream as identified by streamId.
// This is currently only supported by streams with "geistapi" or "pubsub" set as source type.
//
// The returned ID string (or resource ID) is dependent on the sink type, but is defined as the key
// to be used for key lookups of the event.
// It is created based on the Sink configuration in the Stream Spec for sink types supporting it:
// 		Firestore: ID is the generated Firestore entity name, as specified in the entityNameFromIds section of the sink spec.
//		BigTable: ID is the generated row-key, as specified in the rowKey section of the sink spec.
// 		Others: currently <noResourceId> or empty string (to be improved in a future update)
func (g *Geist) Publish(ctx context.Context, streamId string, event []byte) (id string, err error) {
	if g.service == nil {
		return id, errors.New(ErrGeistNotInitialized)
	}

	stream, err := g.service.Stream(streamId)
	if err != nil {
		return id, errors.New(ErrInvalidStreamId + ", details: " + err.Error())
	}

	// Spec Registration events should not use this method, but RegisterStream instead to
	// ensure proper spec validation.
	if streamId == g.service.Registry().StreamId() {
		return id, errors.New(ErrCodeInvalidSpecRegOp)
	}

	id, err = stream.Publish(ctx, event)

	if err != nil {
		return id, errors.New(ErrInternalDataProcessing + ", details: " + err.Error())
	}
	return
}

func (g *Geist) GetStreamSpec(streamId string) (specData []byte, err error) {
	stream, err := g.service.Stream(streamId)
	if err == nil {
		specData = stream.Spec().JSON()
	}
	return
}

func (g *Geist) GetStreamSpecs(ctx context.Context) (specs map[string][]byte, err error) {
	specs = make(map[string][]byte)
	specsFromReg, err := g.service.Registry().GetAll(ctx)
	if err != nil {
		return nil, err
	}

	for id, spec := range specsFromReg {
		specs[id] = spec.JSON()
	}
	return
}

func (g *Geist) ValidateStreamSpec(specData []byte) (specId string, err error) {

	spec, err := g.service.Registry().Validate(specData)
	if err != nil {
		return specId, errors.New(ErrInvalidStreamSpec + ", details: " + err.Error())
	}

	if spec.Id() == g.service.Registry().StreamId() {
		// Updates to GEIST's internal spec registration stream are not allowed
		return specId, errors.New(ErrProtectedStreamId + ", details: " + err.Error())
	}

	return spec.Id(), err
}

func (g *Geist) Shutdown(ctx context.Context) (err error) {
	if g.service == nil {
		return errors.New(ErrGeistNotInitialized)
	}
	g.cancel()
	g.service.Shutdown(ctx, err)
	return
}
