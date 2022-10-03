package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/teltech/logger"
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/internal/pkg/admin"
	"github.com/zpiroux/geist/internal/pkg/igeist"
	"github.com/zpiroux/geist/internal/pkg/notify"
)

// Regardless of DB implementation for Registry, it requires the ETL spec to use
// RawEventField as the key for storing each spec.
const RawEventField = "rawEvent"

type Config struct {
	StorageMode admin.RegStorageMode
	RegSpec     []byte // currently only needed outside during initialization of StreamRegistry
}

// StreamRegistry implements both the Registry and the Executor interfaces so that it can serve
// both as the bootstrap/CRUD Registry service for the supervisor, and as the continuous spec registration
// service via GEIST REST API, ingesting specs in an ETL Stream with arbitrary Sinks/Loaders.
type StreamRegistry struct {
	config      Config
	loader      entity.Loader          // where to store the specs
	executor    igeist.Executor        // internal handling of stream registrations
	adminStream igeist.Stream          // for sending registry change events
	specs       map[string]igeist.Spec // in-mem cache of specs
	notifier    *notify.Notifier
}

func NewStreamRegistry(config Config, executor igeist.Executor, notifyChan entity.NotifyChan, logging bool) *StreamRegistry {

	sr := &StreamRegistry{
		config:   config,
		specs:    make(map[string]igeist.Spec),
		executor: executor,
	}

	var log *logger.Log
	if logging {
		log = logger.New()
	}
	sr.notifier = notify.New(notifyChan, log, 2, "streamregistry", sr.StreamId(), "")

	return sr
}

func (r *StreamRegistry) SetLoader(loader entity.Loader) {
	r.loader = loader
}

//
// Registry impl funcs
//

// The StreamRegistry implementation of Registry.Put() only caches the spec in-memory,
// since the actual persistence of specs are done with its Stream ETL Executor implementation
// inside ProcessEvent(), which in turn relies on the assigned Loader to store the spec.
// It is currently only used internally from StreamRegistry's ProcessEvent function as
// part of Supervisor managed updated of new specs, thus no mutex needed on specs map.
func (r *StreamRegistry) Put(ctx context.Context, id string, spec igeist.Spec) error {
	r.specs[id] = spec
	return nil
}

func (r *StreamRegistry) Fetch(ctx context.Context) error {
	var updatedSpecs []*entity.Transformed

	// If we're using in-memory storage mode we don't need to fetch anything. All specs
	// are already cached in-mem from the ProcessEvent/Put ops.
	if r.config.StorageMode == admin.RegStorageInMemory {
		return nil
	}

	query := entity.ExtractorQuery{Type: entity.QueryTypeAll}
	err, retryable := r.executor.Stream().ExtractFromSink(ctx, query, &updatedSpecs)

	if err != nil {
		_ = retryable // TODO: Handle retryable
		return err
	}

	for _, specData := range updatedSpecs {
		rawData := []byte(specData.Data[RawEventField].(string))
		spec, err := entity.NewSpec(rawData)
		if err != nil {
			r.notifier.Notify(entity.NotifyLevelError, "stored spec is corrupt and will be disregarded, err: %s, specData: %s", err.Error(), string(rawData))
			continue
		}

		r.specs[spec.Id()] = spec
	}

	return nil
}

func (r *StreamRegistry) Get(ctx context.Context, id string) (igeist.Spec, error) {
	return r.specs[id], nil
}

func (r *StreamRegistry) GetAll(ctx context.Context) (map[string]igeist.Spec, error) {
	return r.specs, nil
}

func (r *StreamRegistry) Delete(ctx context.Context, id string) error {
	return errors.New("not implemented")
}

func (r *StreamRegistry) Exists(id string) bool {
	_, exists := r.specs[id]
	return exists
}

func (r *StreamRegistry) ExistsSameVersion(specBytes []byte) (bool, error) {
	spec, err := entity.NewSpec(specBytes)
	if err != nil {
		return false, err
	}
	existingSpec, exists := r.specs[spec.Id()]
	if !exists {
		return false, nil
	}

	if spec.Version == existingSpec.(*entity.Spec).Version {
		return true, nil
	}

	return false, nil
}

func (r *StreamRegistry) Validate(specBytes []byte) (igeist.Spec, error) {
	return entity.NewSpec(specBytes)
}

func (r *StreamRegistry) SetAdminStream(stream igeist.Stream) {
	r.adminStream = stream
}

//
// Executor impl funcs, executing in a separate goroutine as part of Supervisor started
// set of Executors, handling the ingestion stream of new/updated specs.
//

func (r *StreamRegistry) StreamId() string {
	return r.executor.StreamId()
}

func (r *StreamRegistry) Stream() igeist.Stream {
	return r.executor.Stream()
}

func (r *StreamRegistry) Run(ctx context.Context, wg *sync.WaitGroup) {
	var (
		err       error
		retryable bool
	)
	defer wg.Done()

	r.notifier.Notify(entity.NotifyLevelInfo, "Executor starting up, with spec: %s", r.executor.StreamId())

	// No need to handle errors and retries here since the only way for the reg stream's extractor to
	// terminate is if the global ctx is canceled due to service shutdown/upgrade.
	r.executor.Stream().Extractor().StreamExtract(ctx, r.ProcessEvent, &err, &retryable)

	if err != nil {
		r.notifier.Notify(entity.NotifyLevelError, "Executor with Stream ID %s finished with err: %v", r.executor.StreamId(), err)
	} else {
		r.notifier.Notify(entity.NotifyLevelInfo, "Executor with Stream ID %s finished successfully", r.executor.StreamId())
	}
}

func (r *StreamRegistry) ProcessEvent(ctx context.Context, events []entity.Event) entity.EventProcessingResult {

	result := r.executor.ProcessEvent(ctx, events)

	if result.Error == nil {

		spec, err := getSpecFromEvent(events)

		if err == nil {
			if spec.Id() != result.ResourceId {
				// Depending on the sink type this might not be an error
				r.notifier.Notify(entity.NotifyLevelWarn, "spec and resource id don't match (%s, %s)", spec.Id(), result.ResourceId)
			}

			err = r.Put(ctx, spec.Id(), spec)
			if err == nil {
				err = r.sendRegistryModifiedEvent(ctx, spec.Id())
			}
		}
		result.Error = err
	}
	return result
}

func (r *StreamRegistry) Shutdown() {
	r.executor.Shutdown()
}

func (r *StreamRegistry) sendRegistryModifiedEvent(ctx context.Context, streamId string) error {

	// Inform all GEIST pods' Supervisors that the Registry repository has been updated with
	// something, e.g. new or updated spec, so they can update cached specs with a new Fetch() and
	// start up any new ETL Streams.
	event := admin.NewAdminEvent(
		admin.EventStreamRegistryModified,
		admin.OperationStreamRegistration,
		streamId)

	eventBytes, err := json.Marshal(event)
	eventId := "none"
	if err == nil {
		eventId, err = r.adminStream.Publish(ctx, eventBytes)
	}
	if err == nil {
		r.notifier.Notify(entity.NotifyLevelInfo, "Successfully published admin event: %+v with id: %s", event, eventId)
	} else {
		r.notifier.Notify(entity.NotifyLevelError, "failed publishing admin event: %+v, err: %v", event, err)
	}

	return err
}

func getSpecFromEvent(events []entity.Event) (*entity.Spec, error) {

	if len(events) == 0 {
		return nil, fmt.Errorf("no event data to create stream spec from")
	}

	spec := entity.NewEmptySpec()
	err := json.Unmarshal(events[0].Data, spec)
	return spec, err
}
