package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	notifyChan := make(entity.NotifyChan, 128)
	go handleNotificationEvents(notifyChan)
	time.Sleep(time.Second)
	config := Config{
		PreTransformHookFunc: preTransformHookFunc,
		NotifyChan:           notifyChan,
	}

	executor := NewExecutor(config, stream)
	assert.Equal(t, entity.Metrics{}, executor.Metrics())

	// Normal flow, no enrichment
	event := HookFuncEvent{TestType: "continue"}
	e := newHookEvent(event)
	streamLoadAttempts = 0
	expectedBytesProcessed := len(e[0].Data)
	result := executor.ProcessEvent(context.Background(), e)
	assert.NoError(t, result.Error)
	assert.Equal(t, properResourceId, result.ResourceId)
	assert.Equal(t, 1, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusSuccessful, result.Status)
	assertEqualMetrics(t, entity.Metrics{
		Microbatches:       1,
		EventsProcessed:    1,
		BytesProcessed:     int64(expectedBytesProcessed),
		EventsStoredInSink: 1,
		SinkOperations:     1,
		BytesIngested:      int64(expectedBytesProcessed),
	}, executor.Metrics())

	// Normal flow, with enrichment (event field modification)
	event = HookFuncEvent{TestType: "enrichAndContinue", SomeInputValue: "foo"}
	streamLoadAttempts = 0
	e = newHookEvent(event)
	expectedBytesProcessed += len(e[0].Data)
	result = executor.ProcessEvent(context.Background(), e)
	assert.NoError(t, result.Error)
	assert.Equal(t, properResourceId, result.ResourceId)
	assert.Equal(t, 1, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusSuccessful, result.Status)
	eventStoredInSink := loader.Data[0].Data["originalEventData"]
	err = json.Unmarshal(eventStoredInSink.([]byte), &event)
	assert.NoError(t, err)
	assert.Equal(t, "foo + myEnrichedValue", event.SomeInputValue)
	expectedBytesIngested := expectedBytesProcessed + len(" + myEnrichedValue")
	assertEqualMetrics(t, entity.Metrics{
		Microbatches:       2,
		EventsProcessed:    2,
		BytesProcessed:     int64(expectedBytesProcessed),
		EventsStoredInSink: 2,
		SinkOperations:     2,
		BytesIngested:      int64(expectedBytesIngested),
	}, executor.Metrics())

	// Normal flow, with enrichment (adding a new field)
	event = HookFuncEvent{TestType: "enrichAndContinue2", SomeInputValue: "bar"}
	streamLoadAttempts = 0
	e = newHookEvent(event)
	expectedBytesProcessed += len(e[0].Data)
	expectedBytesIngested += len(e[0].Data)
	result = executor.ProcessEvent(context.Background(), e)
	assert.NoError(t, result.Error)
	assert.Equal(t, properResourceId, result.ResourceId)
	assert.Equal(t, 1, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusSuccessful, result.Status)
	eventStoredInSink = loader.Data[0].Data["originalEventData"]
	expectedEnrichedEventInSink := "{\"TestType\":\"enrichAndContinue2\",\"SomeInputValue\":\"bar\",\"myInjectedField\":\"coolValue\"}"
	expectedBytesIngested = expectedBytesIngested + len(",\"myInjectedField\":\"coolValue\"")
	assert.Equal(t, expectedEnrichedEventInSink, string(eventStoredInSink.([]byte)))
	assertEqualMetrics(t, entity.Metrics{
		Microbatches:       3,
		EventsProcessed:    3,
		BytesProcessed:     int64(expectedBytesProcessed),
		EventsStoredInSink: 3,
		SinkOperations:     3,
		BytesIngested:      int64(expectedBytesIngested),
	}, executor.Metrics())

	// HookFunc decides event to be skipped
	event.TestType = "skip"
	streamLoadAttempts = 0
	e = newHookEvent(event)
	expectedBytesProcessed += len(e[0].Data)
	result = executor.ProcessEvent(context.Background(), e)
	assert.NoError(t, result.Error)
	assert.Equal(t, "", result.ResourceId)
	assert.Equal(t, 0, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusSuccessful, result.Status)
	assertEqualMetrics(t, entity.Metrics{
		Microbatches:       4,
		EventsProcessed:    4,
		BytesProcessed:     int64(expectedBytesProcessed),
		EventsStoredInSink: 3,
		SinkOperations:     3,
		BytesIngested:      int64(expectedBytesIngested),
	}, executor.Metrics())

	// HookFunc decides it want to report back an unretryable error
	event.TestType = "unretryableError"
	streamLoadAttempts = 0
	e = newHookEvent(event)
	expectedBytesProcessed += len(e[0].Data)
	result = executor.ProcessEvent(context.Background(), e)
	assert.EqualError(t, result.Error, ErrHookUnretryableError.Error())
	assert.Equal(t, "", result.ResourceId)
	assert.Equal(t, 0, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusError, result.Status)
	assert.False(t, result.Retryable)
	assertEqualMetrics(t, entity.Metrics{
		Microbatches:       5,
		EventsProcessed:    5,
		BytesProcessed:     int64(expectedBytesProcessed),
		EventsStoredInSink: 3,
		SinkOperations:     3,
		BytesIngested:      int64(expectedBytesIngested),
	}, executor.Metrics())

	// HookFunc decides the stream should be terminated
	event.TestType = "shutdown"
	streamLoadAttempts = 0
	e = newHookEvent(event)
	expectedBytesProcessed += len(e[0].Data)
	result = executor.ProcessEvent(context.Background(), e)
	assert.NoError(t, result.Error)
	assert.Equal(t, "", result.ResourceId)
	assert.Equal(t, 0, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusShutdown, result.Status)
	assert.False(t, result.Retryable)
	assertEqualMetrics(t, entity.Metrics{
		Microbatches:       6,
		EventsProcessed:    6,
		BytesProcessed:     int64(expectedBytesProcessed),
		EventsStoredInSink: 3,
		SinkOperations:     3,
		BytesIngested:      int64(expectedBytesIngested),
	}, executor.Metrics())

	// HookFunc has a bug and returns invalid action value
	event.TestType = "invalidReturnValue"
	streamLoadAttempts = 0
	e = newHookEvent(event)
	expectedBytesProcessed += len(e[0].Data)
	result = executor.ProcessEvent(context.Background(), e)
	assert.Error(t, result.Error)
	assert.True(t, errors.Is(result.Error, ErrHookInvalidAction))
	assert.Equal(t, "", result.ResourceId)
	assert.Equal(t, 0, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusError, result.Status)
	assert.False(t, result.Retryable)
	assertEqualMetrics(t, entity.Metrics{
		Microbatches:       7,
		EventsProcessed:    7,
		BytesProcessed:     int64(expectedBytesProcessed),
		EventsStoredInSink: 3,
		SinkOperations:     3,
		BytesIngested:      int64(expectedBytesIngested),
	}, executor.Metrics())

	// Check received stream ID
	event.TestType = "enrichWithStreamId"
	streamLoadAttempts = 0
	e = newHookEvent(event)
	expectedBytesProcessed += len(e[0].Data)
	result = executor.ProcessEvent(context.Background(), e)
	assert.NoError(t, result.Error)
	assert.Equal(t, entity.ExecutorStatusSuccessful, result.Status)
	assert.Equal(t, 1, streamLoadAttempts)
	eventStoredInSink = loader.Data[0].Data["originalEventData"]
	err = json.Unmarshal(eventStoredInSink.([]byte), &event)
	assert.NoError(t, err)
	assert.Equal(t, spec.Id(), event.SomeInputValue)
	eventStoredInSink = loader.Data[0].Data["originalEventData"]
	expectedEnrichedEventInSink = "{\"TestType\":\"enrichWithStreamId\",\"SomeInputValue\":\"geisttest-minspec\"}"
	expectedBytesIngested = expectedBytesIngested + len(expectedEnrichedEventInSink)
	assert.Equal(t, expectedEnrichedEventInSink, string(eventStoredInSink.([]byte)))

	m := executor.Metrics()
	assertEqualMetrics(t, entity.Metrics{
		Microbatches:       8,
		EventsProcessed:    8,
		BytesProcessed:     int64(expectedBytesProcessed),
		EventsStoredInSink: 4,
		SinkOperations:     4,
		BytesIngested:      int64(expectedBytesIngested),
	}, m)

	assert.True(t, m.EventProcessingTimeMicros > 0)
	assert.True(t, m.SinkProcessingTimeMicros > 0, m)

	streamLoadAttempts = 0

	time.Sleep(2 * time.Second)
}

func handleNotificationEvents(notifyChan entity.NotifyChan) {
	for event := range notifyChan {
		fmt.Printf("%+v\n", event)
	}
}

func newHookEvent(he HookFuncEvent) []entity.Event {
	eventBytes, _ := json.Marshal(he)
	return []entity.Event{{Data: eventBytes, Ts: time.Now(), Key: nil}}
}

type HookFuncEvent struct {
	TestType       string
	SomeInputValue string
}

func preTransformHookFunc(ctx context.Context, spec *entity.Spec, event *[]byte) entity.HookAction {

	var (
		e   HookFuncEvent
		err error
	)
	_ = json.Unmarshal(*event, &e)

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
	case "enrichWithStreamId":
		*event, err = sjson.SetBytes(*event, "SomeInputValue", spec.Id())
		if err != nil {
			return entity.HookActionUnretryableError
		}
		return entity.HookActionProceed
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
	cfg := Config{
		NotifyChan: make(entity.NotifyChan, 6),
		Log:        false,
	}
	go handleNotificationEvents(cfg.NotifyChan)
	time.Sleep(time.Second)

	executor := NewExecutor(cfg, stream)
	assert.NotNil(t, executor)

	var wg sync.WaitGroup
	wg.Add(1)
	executor.Run(context.Background(), &wg)
	wg.Wait()

	// Loader/Sink resilience
	stream.loader = &PanickingMockLoader{}
	_ = executor.ProcessEvent(context.Background(), tinyTestEvent(time.Now()))
	m := executor.Metrics()
	assertEqualMetrics(t, entity.Metrics{
		Microbatches:       1,
		EventsProcessed:    1,
		BytesProcessed:     int64(len(tinyEvent)),
		EventsStoredInSink: 0,
		SinkOperations:     0,
		BytesIngested:      0,
	}, m)

	assert.True(t, m.EventProcessingTimeMicros > 0)
	assert.Zero(t, m.SinkProcessingTimeMicros)

	time.Sleep(time.Second)
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
	time.Sleep(time.Millisecond)
	x := 0
	y := 1 / x
	_ = y
	return "nope", nil, false
}

func (p *PanickingMockLoader) Shutdown(ctx context.Context) {
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

	notifyChan := make(entity.NotifyChan, 128)
	go handleNotificationEvents(notifyChan)
	time.Sleep(time.Second)
	config := Config{
		NotifyChan: notifyChan,
		Log:        false,
	}
	executor := NewExecutor(config, stream)

	// Test happy path
	result := executor.ProcessEvent(context.Background(), tinyTestEvent(time.Now()))
	assert.NoError(t, result.Error)
	assert.Equal(t, properResourceId, result.ResourceId)
	assert.Equal(t, 1, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusSuccessful, result.Status)
	bytesProcessed := int64(len(tinyEvent))
	assertEqualMetrics(t, entity.Metrics{
		Microbatches:       1,
		EventsProcessed:    1,
		BytesProcessed:     bytesProcessed,
		EventsStoredInSink: 1,
		SinkOperations:     1,
		BytesIngested:      bytesProcessed,
	}, executor.Metrics())

	// Test correct result when nothing to transform
	stream.transformer = &MockTransformer_NothingToTransform{}
	executor.stream = stream
	result = executor.ProcessEvent(context.Background(), tinyTestEvent(time.Now()))
	assert.NoError(t, result.Error)
	assert.Equal(t, entity.ExecutorStatusSuccessful, result.Status)
	bytesIngested := bytesProcessed
	bytesProcessed += int64(len(tinyEvent))
	assertEqualMetrics(t, entity.Metrics{
		Microbatches:       2,
		EventsProcessed:    2,
		BytesProcessed:     bytesProcessed,
		EventsStoredInSink: 1,
		SinkOperations:     1,
		BytesIngested:      bytesIngested,
	}, executor.Metrics())

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
	bytesProcessed += int64(len(tinyEvent))
	assertEqualMetrics(t, entity.Metrics{
		Microbatches:       3,
		EventsProcessed:    3,
		BytesProcessed:     bytesProcessed,
		EventsStoredInSink: 1,
		SinkOperations:     1,
		BytesIngested:      bytesIngested,
	}, executor.Metrics())

	// Test handling of retryable error
	stream.loader = &MockLoader_RetryableError{}
	streamLoadAttempts = 0
	executor.stream = stream
	result = executor.ProcessEvent(context.Background(), tinyTestEvent(time.Now()))
	assert.Error(t, result.Error)
	assert.Equal(t, noResourceId, result.ResourceId)
	assert.Equal(t, entity.DefaultMaxEventProcessingRetries+1, streamLoadAttempts)
	assert.Equal(t, entity.ExecutorStatusRetriesExhausted, result.Status)
	bytesProcessed += int64(len(tinyEvent))
	assertEqualMetrics(t, entity.Metrics{
		Microbatches:       4,
		EventsProcessed:    4,
		BytesProcessed:     bytesProcessed,
		EventsStoredInSink: 1,
		SinkOperations:     1,
		BytesIngested:      bytesIngested,
	}, executor.Metrics())
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
	eventBytes := int64(len(tinyEvent) + len(tinyEvent2))
	assertEqualMetrics(t, entity.Metrics{
		Microbatches:       1,
		EventsProcessed:    2,
		BytesProcessed:     eventBytes,
		EventsStoredInSink: 2,
		SinkOperations:     1,
		BytesIngested:      eventBytes,
	}, executor.Metrics())

	executor.Shutdown(context.Background())
	time.Sleep(time.Second)
}

func TestValid(t *testing.T) {
	var e Executor
	assert.False(t, e.valid())
	e.stream = &Stream{}
	assert.False(t, e.valid())
}

func assertEqualMetrics(t *testing.T, expected, actual entity.Metrics) {
	assert.Equal(t, expected.Microbatches, actual.Microbatches, "Microbatches")
	assert.Equal(t, expected.EventsProcessed, actual.EventsProcessed, "EventsProcessed")
	assert.Equal(t, expected.BytesProcessed, actual.BytesProcessed, "BytesProcessed")
	assert.Equal(t, expected.EventsStoredInSink, actual.EventsStoredInSink, "EventsStoredInSink")
	assert.Equal(t, expected.SinkOperations, actual.SinkOperations, "SinkOperations")
	assert.Equal(t, expected.BytesIngested, actual.BytesIngested, "BytesIngested")
}

type MockLoader_StoreLatest struct {
	Data []*entity.Transformed
}

func (s *MockLoader_StoreLatest) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {
	streamLoadAttempts++
	s.Data = data
	time.Sleep(time.Millisecond)
	return properResourceId, nil, false
}

func (s *MockLoader_StoreLatest) Shutdown(ctx context.Context) {
	// nothing to mock here
}

type MockLoader_NoError struct{}

func (s *MockLoader_NoError) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {
	streamLoadAttempts++
	return properResourceId, nil, false
}

func (s *MockLoader_NoError) Shutdown(ctx context.Context) {
	// nothing to mock here
}

type MockLoader_Error struct{}

func (s *MockLoader_Error) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {
	streamLoadAttempts++
	time.Sleep(time.Millisecond)
	return noResourceId, errors.New("something bad happened"), false
}

func (s *MockLoader_Error) Shutdown(ctx context.Context) {
	// nothing to mock here
}

type MockLoader_RetryableError struct{}

func (s *MockLoader_RetryableError) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {
	streamLoadAttempts++
	return noResourceId, errors.New("something bad happened"), true
}

func (s *MockLoader_RetryableError) Shutdown(ctx context.Context) {
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
