package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tidwall/sjson"
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/internal/pkg/etltest"
)

const (
	properResourceId = "properResourceId"
	noResourceId     = "noResourceId"
)

var minimalStreamSpec = []byte(`
{
   "namespace": "geisttest",
   "streamIdSuffix": "minspec",
   "description": "A minimal spec for mocking",
   "version": 1,
   "source": {
      "type": "geistapi"
   },
   "transform": {},
   "sink": {
      "type": "void"
   }
}`)

var tinyEvent = []byte(`
{
   "coolField": "coolValue",
}`)

var tinyEvent2 = []byte(`
{
   "coolerField": "coolerValue",
}`)

func tinyTestEvent(ts time.Time) []entity.Event {
	return []entity.Event{{Data: tinyEvent, Ts: ts, Key: nil}}
}

var streamLoadAttempts int

// Test Hook logic
func TestExecutorHookLogic(t *testing.T) {

	spec, err := entity.NewSpec(minimalStreamSpec)
	assert.NoError(t, err)
	transformer := NewMockTransformer(spec.Transform)
	loader := &MockLoader_StoreLatest{}
	stream := NewStream(
		spec,
		"teststream",
		etltest.NewMockExtractor(spec.Source.Config),
		transformer,
		loader,
		nil,
	)

	executor := NewExecutor(Config{PreTransformHookFunc: preTransformHookFunc}, stream)

	// Normal flow, no enrichment
	event := HookFuncEvent{TestType: "continue"}
	streamLoadAttempts = 0
	result := executor.ProcessEvent(context.Background(), newHookEvent(event))
	assert.NoError(t, result.Error)
	assert.Equal(t, properResourceId, result.ResourceId)
	assert.Equal(t, 1, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusSuccessful, result.Status)

	// Normal flow, with enrichment (event field modification)
	event = HookFuncEvent{TestType: "enrichAndContinue", SomeInputValue: "foo"}
	streamLoadAttempts = 0
	e := newHookEvent(event)
	result = executor.ProcessEvent(context.Background(), e)
	assert.NoError(t, result.Error)
	assert.Equal(t, properResourceId, result.ResourceId)
	assert.Equal(t, 1, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusSuccessful, result.Status)
	eventStoredInSink := loader.Data[0].Data["originalEventData"]
	err = json.Unmarshal(eventStoredInSink.([]byte), &event)
	assert.NoError(t, err)
	assert.Equal(t, "foo + myEnrichedValue", event.SomeInputValue)

	// Normal flow, with enrichment (adding a new field)
	event = HookFuncEvent{TestType: "enrichAndContinue2", SomeInputValue: "bar"}
	streamLoadAttempts = 0
	e = newHookEvent(event)
	result = executor.ProcessEvent(context.Background(), e)
	assert.NoError(t, result.Error)
	assert.Equal(t, properResourceId, result.ResourceId)
	assert.Equal(t, 1, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusSuccessful, result.Status)
	eventStoredInSink = loader.Data[0].Data["originalEventData"]
	expectedEnrichedEventInSink := "{\"TestType\":\"enrichAndContinue2\",\"SomeInputValue\":\"bar\",\"myInjectedField\":\"coolValue\"}"
	assert.Equal(t, expectedEnrichedEventInSink, string(eventStoredInSink.([]byte)))

	// HookFunc decides event to be skipped
	event.TestType = "skip"
	streamLoadAttempts = 0
	result = executor.ProcessEvent(context.Background(), newHookEvent(event))
	assert.NoError(t, result.Error)
	assert.Equal(t, "", result.ResourceId)
	assert.Equal(t, 0, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusSuccessful, result.Status)

	// HookFunc decides it want to report back an unretryable error
	event.TestType = "unretryableError"
	streamLoadAttempts = 0
	result = executor.ProcessEvent(context.Background(), newHookEvent(event))
	assert.EqualError(t, result.Error, ErrHookUnretryableError.Error())
	assert.Equal(t, "", result.ResourceId)
	assert.Equal(t, 0, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusError, result.Status)
	assert.False(t, result.Retryable)

	// HookFunc decides the stream should be terminated
	event.TestType = "shutdown"
	streamLoadAttempts = 0
	result = executor.ProcessEvent(context.Background(), newHookEvent(event))
	assert.NoError(t, result.Error)
	assert.Equal(t, "", result.ResourceId)
	assert.Equal(t, 0, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusShutdown, result.Status)
	assert.False(t, result.Retryable)

	// HookFunc has a bug and returns invalid action value
	event.TestType = "invalidReturnValue"
	streamLoadAttempts = 0
	result = executor.ProcessEvent(context.Background(), newHookEvent(event))
	assert.Error(t, result.Error)
	assert.True(t, errors.Is(result.Error, ErrHookInvalidAction))
	assert.Equal(t, "", result.ResourceId)
	assert.Equal(t, 0, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusError, result.Status)
	assert.False(t, result.Retryable)
}

func newHookEvent(he HookFuncEvent) []entity.Event {
	eventBytes, _ := json.Marshal(he)
	return []entity.Event{{Data: eventBytes, Ts: time.Now(), Key: nil}}
}

type HookFuncEvent struct {
	TestType       string
	SomeInputValue string
}

func preTransformHookFunc(ctx context.Context, event *[]byte) entity.HookAction {

	var (
		e   HookFuncEvent
		err error
	)
	json.Unmarshal(*event, &e)

	switch e.TestType {
	case "continue":
		return entity.HookActionProceed
	case "enrichAndContinue":
		// Modify value in existing field in event
		e.SomeInputValue += " + myEnrichedValue"
		*event, err = json.Marshal(e)
		if err != nil {
			return entity.HookActionUnretryableError
		}
		return entity.HookActionProceed
	case "enrichAndContinue2":
		// Inject a completely new field in the event
		*event, err = sjson.SetBytes(*event, "myInjectedField", "coolValue")
		if err != nil {
			return entity.HookActionUnretryableError
		}
		return entity.HookActionProceed
	case "skip":
		return entity.HookActionSkip
	case "unretryableError":
		return entity.HookActionUnretryableError
	case "shutdown":
		return entity.HookActionShutdown
	case "invalidReturnValue":
		return 76395
	default:
		return entity.HookActionInvalid
	}
}

// Ensure Executor is resilient against bad extractor/source types
func TestExecutorConnectorResilience(t *testing.T) {

	// Extractor/Source resilience
	spec, err := entity.NewSpec(minimalStreamSpec)
	assert.NoError(t, err)
	transformer := NewMockTransformer(spec.Transform)
	extractor := NewPanickingMockExtractor(spec.Source.Config)
	stream := NewStream(
		spec,
		"teststream",
		extractor,
		transformer,
		&MockLoader_NoError{},
		nil,
	)
	executor := NewExecutor(Config{}, stream)
	assert.NotNil(t, executor)

	var wg sync.WaitGroup
	wg.Add(1)
	executor.Run(context.Background(), &wg)
	wg.Wait()

	// Loader/Sink resilience
	stream.loader = &PanickingMockLoader{}
	_ = executor.ProcessEvent(context.Background(), tinyTestEvent(time.Now()))
}

type PanickingMockExtractor struct {
	spec entity.SourceConfig
}

func NewPanickingMockExtractor(spec entity.SourceConfig) *PanickingMockExtractor {
	return &PanickingMockExtractor{
		spec: spec,
	}
}

func (m *PanickingMockExtractor) StreamExtract(
	ctx context.Context,
	reportEvent entity.ProcessEventFunc,
	err *error,
	retryable *bool) {

	var i *int
	*i = 1
}

func (m *PanickingMockExtractor) Extract(ctx context.Context, query entity.ExtractorQuery, result any) (error, bool) {
	return nil, false
}

func (m *PanickingMockExtractor) ExtractFromSink(ctx context.Context, query entity.ExtractorQuery, result *[]*entity.Transformed) (error, bool) {
	return nil, false
}

func (m *PanickingMockExtractor) SendToSource(ctx context.Context, eventData any) (string, error) {
	return "", nil
}

type PanickingMockLoader struct{}

func (p *PanickingMockLoader) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {
	x := 0
	y := 1 / x
	_ = y
	return "nope", nil, false
}

func (p *PanickingMockLoader) Shutdown() {
	// nothing to mock here
}

// Test sink handling logic
func TestExecutorProcessEvent(t *testing.T) {

	spec, err := entity.NewSpec(minimalStreamSpec)
	assert.NoError(t, err)
	transformer := NewMockTransformer(spec.Transform)
	stream := NewStream(
		spec,
		"teststream",
		etltest.NewMockExtractor(spec.Source.Config),
		transformer,
		&MockLoader_NoError{},
		etltest.NewMockExtractor(spec.Source.Config), // not used, but should be changed to full spec
	)
	executor := NewExecutor(Config{}, stream)

	// Test happy path
	result := executor.ProcessEvent(context.Background(), tinyTestEvent(time.Now()))
	assert.NoError(t, result.Error)
	assert.Equal(t, properResourceId, result.ResourceId)
	assert.Equal(t, 1, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusSuccessful, result.Status)

	// Test correct result when nothing to transform
	stream.transformer = &MockTransformer_NothingToTransform{}
	executor.stream = stream
	result = executor.ProcessEvent(context.Background(), tinyTestEvent(time.Now()))
	assert.NoError(t, result.Error)
	assert.Equal(t, entity.ExecutorStatusSuccessful, result.Status)

	// Test handling of non-retryable error
	stream.loader = &MockLoader_Error{}
	stream.transformer = transformer
	streamLoadAttempts = 0
	executor.stream = stream
	result = executor.ProcessEvent(context.Background(), tinyTestEvent(time.Now()))
	assert.Error(t, result.Error)
	assert.Equal(t, noResourceId, result.ResourceId)
	assert.Equal(t, 1, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusError, result.Status)

	// Test handling of retryable error
	stream.loader = &MockLoader_RetryableError{}
	streamLoadAttempts = 0
	executor.stream = stream
	result = executor.ProcessEvent(context.Background(), tinyTestEvent(time.Now()))
	assert.Error(t, result.Error)
	assert.Equal(t, noResourceId, result.ResourceId)
	assert.Equal(t, entity.DefaultMaxEventProcessingRetries+1, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusRetriesExhausted, result.Status)
}

// Test processing of multiple events in a single call
func TestExecutorProcessMultiEvent(t *testing.T) {

	spec, err := entity.NewSpec(minimalStreamSpec)
	assert.NoError(t, err)
	transformer := NewMockTransformer(spec.Transform)
	loader := &MockLoader_StoreLatest{}
	stream := NewStream(
		spec,
		"teststream",
		etltest.NewMockExtractor(spec.Source.Config),
		transformer,
		loader,
		etltest.NewMockExtractor(spec.Source.Config),
	)
	executor := NewExecutor(Config{}, stream)

	streamLoadAttempts = 0
	executor.stream = stream
	ts := time.Date(2020, time.Month(4), 27, 23, 48, 20, 0, time.UTC)
	events := []entity.Event{{Data: tinyEvent, Ts: ts, Key: []byte("foo")}, {Data: tinyEvent2, Ts: ts, Key: []byte("bar")}}
	result := executor.ProcessEvent(context.Background(), events)
	assert.NoError(t, result.Error)
	assert.Equal(t, properResourceId, result.ResourceId)
	assert.Equal(t, 1, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusSuccessful, result.Status)
	assert.Equal(t, tinyEvent, loader.Data[0].Data["originalEventData"])
	assert.Equal(t, tinyEvent2, loader.Data[1].Data["originalEventData"])
}

func TestValid(t *testing.T) {
	var e Executor
	assert.False(t, e.valid())
	e.stream = &Stream{}
	assert.False(t, e.valid())
}

type MockLoader_StoreLatest struct {
	Data []*entity.Transformed
}

func (s *MockLoader_StoreLatest) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {
	streamLoadAttempts++
	s.Data = data
	return properResourceId, nil, false
}

func (s *MockLoader_StoreLatest) Shutdown() {
	// nothing to mock here
}

type MockLoader_NoError struct{}

func (s *MockLoader_NoError) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {
	streamLoadAttempts++
	return properResourceId, nil, false
}

func (s *MockLoader_NoError) Shutdown() {
	// nothing to mock here
}

type MockLoader_Error struct{}

func (s *MockLoader_Error) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {
	streamLoadAttempts++
	return noResourceId, errors.New("something bad happened"), false
}

func (s *MockLoader_Error) Shutdown() {
	// nothing to mock here
}

type MockLoader_RetryableError struct{}

func (s *MockLoader_RetryableError) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {
	streamLoadAttempts++
	return noResourceId, errors.New("something bad happened"), true
}

func (s *MockLoader_RetryableError) Shutdown() {
	// nothing to mock here
}

type MockTransformer struct {
	spec entity.Transform
}

func NewMockTransformer(spec entity.Transform) *MockTransformer {
	return &MockTransformer{
		spec: spec,
	}
}

func (s *MockTransformer) Transform(
	ctx context.Context,
	event []byte,
	retryable *bool) ([]*entity.Transformed, error) {

	transformed := entity.NewTransformed()

	transformed.Data["originalEventData"] = event
	transformed.Data["resultFromTransformation"] = "cool transform of '" + string(event) + "'"

	return []*entity.Transformed{transformed}, nil
}

type MockTransformer_NothingToTransform struct{}

func (s *MockTransformer_NothingToTransform) Transform(
	ctx context.Context,
	event []byte,
	retryable *bool) ([]*entity.Transformed, error) {

	return nil, nil
}
