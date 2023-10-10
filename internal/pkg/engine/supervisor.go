package engine

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
	"github.com/zpiroux/geist/pkg/notify"
)

// Supervisor is responsible for high-level lifecycle management of Geist streams. It initializes and starts up one
// or more Executor(s) per Stream Spec, in its own goroutine. Each Executor is given a newly created Stream entity
// comprising Extractor, Transformer and Loader objects, based on Spec and Deployment config.
type Supervisor struct {
	config        Config
	registry      igeist.StreamRegistry
	streamBuilder igeist.StreamBuilder
	eventHandler  entity.Loader
	archivist     *executorArchivist
	wgExecutors   sync.WaitGroup
	instanceId    string
	notifier      *notify.Notifier
}

// Supervisor expects the provided registry to be initialized with cached specs
func NewSupervisor(
	ctx context.Context,
	config Config,
	streamBuilder igeist.StreamBuilder,
	registry igeist.StreamRegistry) (*Supervisor, error) {

	supervisor := &Supervisor{
		config:        config,
		streamBuilder: streamBuilder,
		archivist:     newExecutorArchivist(),
	}

	supervisor.instanceId = registry.Stream().Instance()
	supervisor.eventHandler = &AdminEventHandler{
		supervisor: supervisor,
		id:         supervisor.instanceId,
	}

	var log *logger.Log
	if config.Log {
		log = logger.New()
	}
	supervisor.notifier = notify.New(config.NotifyChan, log, 2, "supervisor", supervisor.instanceId, "")

	supervisor.setRegistry(ctx, registry)

	return supervisor, nil
}

func (s *Supervisor) Init(ctx context.Context) error {
	specs, err := s.registry.GetAll(ctx)
	if err != nil {
		return err
	}

	for _, spec := range specs {
		if spec.IsDisabled() {
			s.notifier.Notify(entity.NotifyLevelInfo, "Stream %s is disabled and will not be assigned to an executor", spec.Id())
			continue
		}
		if err := s.createStreams(ctx, spec); err != nil {
			return err
		}
	}
	return nil
}

// Registry returns the registry containing all registered stream specs
func (s *Supervisor) Registry() igeist.StreamRegistry {
	return s.registry
}

// Run is the main entry point for GEIST execution of all streams
func (s *Supervisor) Run(ctx context.Context, ready *sync.WaitGroup) error {
	s.notifier.Notify(entity.NotifyLevelInfo, "Starting up with config: %+v", s.config)

	nbExecutorsDeployed := s.deployAllStreams(ctx)
	s.notifier.Notify(entity.NotifyLevelInfo, "All (%d) executors deployed", nbExecutorsDeployed)

	// Everything is up and running, including all previously registered streams.
	ready.Done()

	// Wait for all stream executors to finish operations
	s.wgExecutors.Wait()
	s.notifier.Notify(entity.NotifyLevelInfo, "All Executors finished operations. Supervisor shutting down.")
	return nil
}

func (s *Supervisor) deployExecutor(ctx context.Context, executor igeist.Executor) {
	s.wgExecutors.Add(1)
	go executor.Run(ctx, &s.wgExecutors)
}

func (s *Supervisor) Metrics() map[string]entity.Metrics {
	var metricsPerStream entity.Metrics
	metrics := make(map[string]entity.Metrics)

	executorMap := s.archivist.GrantExclusiveAccess()
	defer s.archivist.RevokeExclusiveAccess()

	for streamId, executors := range *executorMap {
		metricsPerStream.Reset()
		for _, executor := range executors {
			if executor.Stream().Spec().IsDisabled() {
				continue
			}
			m := executor.Metrics()
			metricsPerStream.EventsProcessed += m.EventsProcessed
			metricsPerStream.EventProcessingTimeMicros += m.EventProcessingTimeMicros
			metricsPerStream.Microbatches += m.Microbatches
			metricsPerStream.BytesProcessed += m.BytesProcessed
			metricsPerStream.EventsStoredInSink += m.EventsStoredInSink
			metricsPerStream.SinkProcessingTimeMicros += m.SinkProcessingTimeMicros
			metricsPerStream.SinkOperations += m.SinkOperations
			metricsPerStream.BytesIngested += m.BytesIngested
		}
		metrics[streamId] = metricsPerStream
	}
	return metrics
}

// Shutdown is called by the service during shutdown
func (s *Supervisor) Shutdown(ctx context.Context) {
	s.notifier.Notify(entity.NotifyLevelInfo, "Shutting down")
}

// Stream returns the first (main) stream instance for a stream id, for use with getting stream spec
// info and stream publishing.
func (s *Supervisor) Stream(id string) (igeist.Stream, error) {
	executors := s.archivist.Get(id)
	if len(executors) == 0 {
		return nil, fmt.Errorf(s.lgprfx()+"stream with id '%s' not found", id)
	}
	return executors[0].Stream(), nil
}

// createStreams creates all stream instances (each managed by its own executor) for
// a given stream ID, based on the config in the provided stream spec.
// If there are active instances already running for that stream ID, those executors
// are shut down, prior to the new ones being created.
// Note that the streams are only created but not yet deployed and running. This is
// done by calling deployStreams().
func (s *Supervisor) createStreams(ctx context.Context, spec *entity.Spec) error {
	executorMap := s.archivist.GrantExclusiveAccess()
	defer s.archivist.RevokeExclusiveAccess()

	s.shutdownExecutors(ctx, executorMap, spec.Id())

	for instance := 1; instance < spec.Ops.StreamsPerPod+1; instance++ {
		stream, err := s.streamBuilder.Build(ctx, spec)
		if err != nil {
			return err
		}

		executor := NewExecutor(s.config, stream)
		if executor == nil {
			return fmt.Errorf(s.lgprfx()+"could not create executor #%d for stream: %#v", instance, spec.Id())
		}
		s.notifier.Notify(entity.NotifyLevelInfo, "Created executor #%d with ID: [%s], for spec with ID: %s", instance, stream.Instance(), spec.Id())

		executors := (*executorMap)[executor.StreamId()]
		executors = append(executors, executor)
		(*executorMap)[executor.StreamId()] = executors
	}
	return nil
}

func (s *Supervisor) shutdownStream(ctx context.Context, streamId string) {
	executorMap := s.archivist.GrantExclusiveAccess()
	defer s.archivist.RevokeExclusiveAccess()
	s.shutdownExecutors(ctx, executorMap, streamId)
}

func (s *Supervisor) shutdownExecutors(ctx context.Context, executorMap *ExecutorMap, streamId string) {
	executors, exists := (*executorMap)[streamId]
	if exists && executors != nil {
		for _, executor := range executors {
			executor.Shutdown(ctx)
		}
		delete(*executorMap, streamId)
		(*executorMap)[streamId] = nil
	} else {
		s.notifier.Notify(entity.NotifyLevelWarn, "shutdownExecutors called for streamId %s but stream did not exist", streamId)
	}
}

func (s *Supervisor) setRegistry(ctx context.Context, registry igeist.StreamRegistry) {
	s.registry = registry
	s.RegisterExecutor(ctx, registry)
}

// RegisterExecutor registers a single executor as the main one for a stream
func (s *Supervisor) RegisterExecutor(ctx context.Context, executor igeist.Executor) {
	executorMap := s.archivist.GrantExclusiveAccess()
	defer s.archivist.RevokeExclusiveAccess()

	id := executor.StreamId()

	executors, exists := (*executorMap)[id]
	if exists && executors != nil {
		for _, executor := range executors {
			executor.Shutdown(ctx)
		}
	}
	executors = append(executors, executor)
	(*executorMap)[id] = executors
}

func (s *Supervisor) handleStreamRegistryModified(ctx context.Context, event admin.AdminEvent) (error, bool) {

	switch event.Data[0].Operation {
	case admin.OperationStreamRegistration:

		err := s.registry.Fetch(ctx)
		if err != nil {
			return err, true
		}
		spec, err := s.registry.Get(ctx, event.Data[0].StreamId)
		if err != nil {
			return err, true
		}
		if isNil(spec) {
			return fmt.Errorf(s.lgprfx() + "registry.Get() returned a nil spec"), false
		}

		if spec.IsDisabled() {
			s.notifier.Notify(entity.NotifyLevelInfo, "New spec version is disabled for streamId '%s', just shutting down old one", spec.Id())
			s.shutdownStream(ctx, spec.Id())
		} else {

			err := s.createStreams(ctx, spec)
			if err != nil {
				return err, false
			}
			s.deployStreams(ctx, spec.Id())
		}
	}
	return nil, false
}

func (s *Supervisor) deployStreams(ctx context.Context, streamId string) {
	executorMap := s.archivist.GrantExclusiveAccess()
	defer s.archivist.RevokeExclusiveAccess()
	executors, exists := (*executorMap)[streamId]
	if exists && executors != nil {
		for _, executor := range executors {
			s.deployExecutor(ctx, executor)
		}
	}
}

func (s *Supervisor) deployAllStreams(ctx context.Context) int {
	var nbExecutorsDeployed int
	executorMap := s.archivist.GrantExclusiveAccess()
	defer s.archivist.RevokeExclusiveAccess()
	for _, executors := range *executorMap {
		for _, executor := range executors {
			s.deployExecutor(ctx, executor)
			nbExecutorsDeployed++
		}
	}
	return nbExecutorsDeployed
}

func (s *Supervisor) lgprfx() string {
	return "[supervisor:" + s.instanceId + "] "
}

// AdminEventHandler returns the event receiver for admin events from pubsub
func (s *Supervisor) AdminEventHandler() entity.Loader {
	return s.eventHandler
}

// AdminEventHandler implements the igeist.Loader interface which will be called in the
// stream where the loader entity is set to 'Admin'. This is the sink in the stream listening
// for admin events.
type AdminEventHandler struct {
	supervisor *Supervisor
	id         string
}

func (a *AdminEventHandler) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {

	if len(data[0].Data) == 0 {
		return "", errors.New(a.lgprfx() + "StreamLoad called without data to load"), false
	}

	eventName := data[0].Data[admin.EventNameKey]
	a.supervisor.notifier.Notify(entity.NotifyLevelInfo, "Admin event received: %s", eventName)

	switch eventName {

	case admin.EventStreamRegistryModified:
		var event admin.AdminEvent
		eventData := data[0].Data[admin.EventRawDataKey]
		err := json.Unmarshal([]byte(eventData.(string)), &event) // TODO: possibly remove string type in admin event spec
		if err != nil {
			return "", err, false
		}
		err, retryable := a.supervisor.handleStreamRegistryModified(ctx, event)
		return "", err, retryable

	default:
		return "", fmt.Errorf(a.lgprfx()+"unsupported admin event receieved: %s", eventName), false
	}
}

func (a *AdminEventHandler) Shutdown(ctx context.Context) {
	// Nothing to shut down
}

func (e *AdminEventHandler) lgprfx() string {
	return "[supervisor:" + e.id + "] "
}

// ExecutorArchivist is the keeper of all running Executors.
type executorArchivist struct {
	x      ExecutorMap
	xMutex *sync.Mutex
}

type ExecutorMap map[string][]igeist.Executor

func newExecutorArchivist() *executorArchivist {
	return &executorArchivist{
		x:      make(ExecutorMap),
		xMutex: &sync.Mutex{},
	}
}

func (e *executorArchivist) Set(executor igeist.Executor) {
	if executor != nil {
		defer e.xMutex.Unlock()
		e.xMutex.Lock()
		executors := e.x[executor.StreamId()]
		executors = append(executors, executor)
		e.x[executor.StreamId()] = executors
	}
}

func (e *executorArchivist) Get(id string) []igeist.Executor {
	defer e.xMutex.Unlock()
	e.xMutex.Lock()
	return e.x[id]
}

func (e *executorArchivist) GrantExclusiveAccess() *ExecutorMap {
	e.xMutex.Lock()
	return &e.x
}

func (e *executorArchivist) RevokeExclusiveAccess() {
	e.xMutex.Unlock()
}
