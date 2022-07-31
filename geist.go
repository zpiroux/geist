package geist

import (
	"context"
	"errors"
	"fmt"

	"github.com/tidwall/sjson"
	"github.com/zpiroux/geist/internal/service"
)

// Error values returned by Geist API.
// Many of these errors will also contain additional details about the error.
// Error matching can still be done with 'if errors.Is(err, ErrInvalidStreamId)' etc.
// due to error wrapping.
var (
	ErrConfigNotInitialized   = errors.New("geist.Config need to be created with NewConfig()")
	ErrGeistNotInitialized    = errors.New("geist not initialized")
	ErrSpecAlreadyExists      = errors.New("stream ID already exists with that version - increment version number to upgrade")
	ErrInvalidStreamSpec      = errors.New("stream Specification is not valid")
	ErrInvalidStreamId        = errors.New("invalid Stream ID")
	ErrProtectedStreamId      = errors.New("spec format is valid but but stream ID is protected and cannot be used")
	ErrCodeInvalidSpecRegOp   = errors.New("use RegisterStream() instead to register new streams")
	ErrInternalDataProcessing = errors.New("internal data processing error")
	ErrInvalidEntityId        = errors.New("invalid source/sink ID")
)

type Geist struct {
	service *service.Service
	cancel  context.CancelFunc
}

// New creates and configures Geist's internal services and all previously registered streams,
// based on the provided config, which needs to be initially created with NewConfig().
func New(ctx context.Context, config *Config) (g *Geist, err error) {
	if config.extractors == nil || config.loaders == nil {
		return nil, ErrConfigNotInitialized
	}
	g = &Geist{}
	g.service, err = service.New(ctx, preProcessConfig(config))
	return g, err
}

// Run starts up Geist's internal services and all previously registered streams
// (if Geist configured to use persistent registry), as prepared by New().
// It is a blocking call until Geist is shut down, from a call to Shutdown or if
// its parent context is canceled.
// Geist must be up and running prior to usage of the other Geist API functions.
func (g *Geist) Run(ctx context.Context) (err error) {
	if g.service == nil {
		return ErrGeistNotInitialized
	}

	ctx, g.cancel = context.WithCancel(ctx)
	return g.service.Run(ctx)
}

// RegisterStream validates and persist the stream spec in the chosen registry implementation.
// If successful, the registered stream is started up immediately, and the generated ID of the stream
// is returned.
func (g *Geist) RegisterStream(ctx context.Context, specData []byte) (id string, err error) {

	g.service.AwaitReady()

	registry := g.service.Registry()
	_, err = registry.Validate(specData)
	if err != nil {
		return id, errWithDetails(ErrInvalidStreamSpec, err)
	}

	exists, err := registry.ExistsSameVersion(specData)

	if err != nil {
		return id, errWithDetails(ErrInternalDataProcessing, err)
	}
	if exists {
		return id, ErrSpecAlreadyExists
	}

	id, err = registry.Stream().Publish(ctx, specData)

	if err != nil {
		return id, errWithDetails(ErrInternalDataProcessing, err)
	}
	return
}

// Publish sends the provided event to the source extractor of the stream as identified
// by streamId, if that extractor type supports this optional functionality.
// Currently known source types that supports this is the internal "geistapi" and the
// GCP source "pubsub".
//
// The returned ID string (or resource ID) is dependent on the sink type, but is defined as the key
// to be used for key lookups of the event.
// It is created based on the Sink configuration in the Stream Spec for sink types supporting it,
// for example:
// 		Firestore: ID is the generated Firestore entity name, as specified in the entityNameFromIds section of the sink spec.
//		BigTable: ID is the generated row-key, as specified in the rowKey section of the sink spec.
func (g *Geist) Publish(ctx context.Context, streamId string, event []byte) (id string, err error) {
	if g.service == nil {
		return id, ErrGeistNotInitialized
	}

	stream, err := g.service.Stream(streamId)
	if err != nil {
		return id, errWithDetails(ErrInvalidStreamId, err)
	}

	// Spec Registration events should not use this method, but RegisterStream instead to
	// ensure proper spec validation.
	if streamId == g.service.Registry().StreamId() {
		return id, ErrCodeInvalidSpecRegOp
	}

	id, err = stream.Publish(ctx, event)

	if err != nil {
		return id, errWithDetails(ErrInternalDataProcessing, err)
	}
	return
}

// GetStreamSpec returns the full stream spec for a specific stream ID
func (g *Geist) GetStreamSpec(streamId string) (specData []byte, err error) {
	stream, err := g.service.Stream(streamId)
	if err == nil {
		specData = stream.Spec().JSON()
	}
	return
}

// GetStreamSpecs returns all registered stream specs
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

// ValidateStreamSpec returns an error if the provided stream spec is invalid.
func (g *Geist) ValidateStreamSpec(specData []byte) (specId string, err error) {

	spec, err := g.service.Registry().Validate(specData)
	if err != nil {
		return specId, errWithDetails(ErrInvalidStreamSpec, err)
	}

	if spec.Id() == g.service.Registry().StreamId() {
		return specId, errWithDetails(ErrProtectedStreamId, err)
	}

	return spec.Id(), err
}

// Shutdown should be called when the app is terminating
func (g *Geist) Shutdown(ctx context.Context) (err error) {
	if g.service == nil {
		return ErrGeistNotInitialized
	}
	g.cancel()
	g.service.Shutdown(ctx, err)
	return
}

// Entities returns IDs of all registered Extractors/Loaders for each Source/Sink.
// The keys for the first map are:
//		"extractor"
//		"loader"
// Each of those keys holds the id/name of the source/sink type that have been registered
//
// Example of output if marshalled to JSON
//
// 		{"extractor":{"source1":true,"source2":true},"loader":{"sink1":true,"sink2":true}}
//
func (g *Geist) Entities() map[string]map[string]bool {
	return g.service.Entities()
}

// EnrichEvent is a convenience function that could be used for event enrichment purposes
// inside a hook function as specified in geist.Config.Hooks.
// It's a wrapper on the sjson package. See doc at https://github.com/tidwall/sjson.
func EnrichEvent(event []byte, path string, value any) ([]byte, error) {
	return sjson.SetBytes(event, path, value)
}

func errWithDetails(err error, errDetails error) error {
	return fmt.Errorf("%w, details: %v", err, errDetails)
}
