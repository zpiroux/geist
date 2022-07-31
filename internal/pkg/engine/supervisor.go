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
)

var log *logger.Log

func init() {
	log = logger.New()
}

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

	supervisor.setRegistry(ctx, registry)

	return supervisor, nil
}

func (s *Supervisor) Init(ctx context.Context) error {

	specs, err := s.registry.GetAll(ctx)
	if err != nil {
		return err
	}
	for _, sp := range specs {

		spec := sp.(*entity.Spec)
		if spec.IsDisabled() {
			log.Infof(s.lgprfx()+"stream %s is disabled and will not be assigned to an executor", spec.Id())
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

	var nbExecutorsDeployed int
	executorMap := s.archivist.GrantExclusiveAccess()
	for _, executors := range *executorMap {
		for _, executor := range executors {
			s.deployExecutor(ctx, executor)
			nbExecutorsDeployed++
		}
	}
	log.Infof(s.lgprfx()+"%d executors deployed", nbExecutorsDeployed)
	s.archivist.RevokeExclusiveAccess()

	// Everything is up and running, including all previously registered streams.
	ready.Done()
	// Wait for all stream executors to finish operations
	s.wgExecutors.Wait()
	log.Info(s.lgprfx() + "All Executors finished operations. Supervisor shutting down.")
	return nil
}

func (s *Supervisor) deployExecutor(ctx context.Context, executor igeist.Executor) {
	s.wgExecutors.Add(1)
	go executor.Run(ctx, &s.wgExecutors)
}

// Shutdown is called by the service during shutdown
func (s *Supervisor) Shutdown(err error) {

	var reason string

	if err == nil {
		reason = "upgrade, client request or similar (no error)"
	} else {
		reason = err.Error()
	}
	log.Infof(s.lgprfx()+"Shutting down. Reason: '%v'", reason)
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

func (s *Supervisor) createStreams(ctx context.Context, spec *entity.Spec) error {

	executorMap := s.archivist.GrantExclusiveAccess()
	defer s.archivist.RevokeExclusiveAccess()

	existingExecutors, exists := (*executorMap)[spec.Id()]
	if exists && existingExecutors != nil {
		for _, executor := range existingExecutors {
			executor.Shutdown()
		}
		(*executorMap)[spec.Id()] = nil
	}

	for instance := 1; instance < spec.Ops.StreamsPerPod+1; instance++ {

		stream, err := s.streamBuilder.Build(ctx, spec)
		if err != nil {
			log.Errorf(s.lgprfx()+"could not build stream from spec: %+v", spec)
			return err
		}

		executor := NewExecutor(s.config, stream)
		if executor == nil {
			return fmt.Errorf(s.lgprfx()+"could not create executor #%d for stream: %#v", instance, spec.Id())
		}
		log.Infof(s.lgprfx()+"Created executor #%d with ID: [%s], for spec with ID: %s", instance, stream.Instance(), spec.Id())

		executors := (*executorMap)[executor.StreamId()]
		executors = append(executors, executor)
		(*executorMap)[executor.StreamId()] = executors
	}
	return nil
}

func (s *Supervisor) shutdownStream(ctx context.Context, streamId string) {
	executors := s.archivist.GrantExclusiveAccess()
	defer s.archivist.RevokeExclusiveAccess()
	existingExecutors, exists := (*executors)[streamId]

	if exists && existingExecutors != nil {
		for _, executor := range existingExecutors {
			executor.Shutdown()
		}
		delete(*executors, streamId)
	} else {
		log.Warnf("shutdownStream called for streamId %s but stream did not exist", streamId)
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
			executor.Shutdown()
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

		streamSpec := spec.(*entity.Spec)
		if streamSpec.IsDisabled() {
			log.Infof(s.lgprfx()+"New spec version is disabled for streamId '%s', just shutting down old one", streamSpec.Id())
			s.shutdownStream(ctx, streamSpec.Id())
		} else {

			err := s.createStreams(ctx, streamSpec)
			if err != nil {
				return err, false
			}
			s.deployStreams(ctx, streamSpec.Id())
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

func (s *Supervisor) lgprfx() string {
	return "[supervisor:" + s.instanceId + "] "
}

// AdminEventHandler returns the event receiver for admin events from pubsub
func (s *Supervisor) AdminEventHandler() entity.Loader {
	return s.eventHandler
}

// AdminEventHandler implements the igeist.Loader interface which will be called in the ETL stream
// where the loader entity is set to 'Admin'. This is the sink in the stream listening for admin pubsub
// events.
type AdminEventHandler struct {
	supervisor *Supervisor
	id         string
}

func (a *AdminEventHandler) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {

	for _, transformed := range data {
		log.Debugf(a.lgprfx()+"Received transformed event in AdminEventHandler.StreamLoad(): %s", transformed.String())
	}

	if len(data[0].Data) == 0 {
		return "", errors.New(a.lgprfx() + "StreamLoad called without data to load"), false
	}

	eventName := data[0].Data[admin.EventNameKey]
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

func (a *AdminEventHandler) Shutdown() {
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
