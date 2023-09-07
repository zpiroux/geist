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
	"github.com/zpiroux/geist/entity/transform"
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

var specForPostTransformHookTest = []byte(`
{
    "namespace": "geisttest",
    "streamIdSuffix": "posttransformhooktest",
    "description": "A spec for testing post transform hooks",
    "version": 1,
	"ops": {
		"logEventData": true
	},
    "source": {
        "type": "geistapi"
    },
    "transform": {
        "extractFields": [
            {
                "fields": [
                    {
                        "id": "testType",
                        "jsonPath": "TestType"
                    }
                ]
            }
        ]
    },
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

var HookLogicTestCases = []struct {
	name                        string
	event                       HookFuncEvent
	expectedIngestion           bool
	expectedEnrichedEventInSink string
	expectedPostHookKeyValue    []string
	expectedStreamLoadAttempts  int
	expectedResult              entity.EventProcessingResult
}{
	{
		name:                       "Normal flow without enrichment",
		event:                      HookFuncEvent{TestType: "continue"},
		expectedIngestion:          true,
		expectedStreamLoadAttempts: 1,
		expectedResult:             entity.EventProcessingResult{Status: entity.ExecutorStatusSuccessful, ResourceId: properResourceId},
	},
	{
		name:                        "Normal flow with enrichment (field modification)",
		event:                       HookFuncEvent{TestType: "modifyExistingField", SomeInputValue: "foo"},
		expectedIngestion:           true,
		expectedEnrichedEventInSink: "{\"TestType\":\"modifyExistingField\",\"SomeInputValue\":\"foo + myEnrichedValue\"}",
		expectedPostHookKeyValue:    []string{"testType", "mutatedTestTypeValue"},
		expectedStreamLoadAttempts:  1,
		expectedResult:              entity.EventProcessingResult{Status: entity.ExecutorStatusSuccessful, ResourceId: properResourceId},
	},
	{
		name:                        "Normal flow with enrichment (adding a new field)",
		event:                       HookFuncEvent{TestType: "injectNewField", SomeInputValue: "bar"},
		expectedIngestion:           true,
		expectedEnrichedEventInSink: "{\"TestType\":\"injectNewField\",\"SomeInputValue\":\"bar\",\"myInjectedField\":\"coolValue\"}",
		expectedPostHookKeyValue:    []string{"myInjectedField", "myInjectedFieldValue"},
		expectedStreamLoadAttempts:  1,
		expectedResult:              entity.EventProcessingResult{Status: entity.ExecutorStatusSuccessful, ResourceId: properResourceId},
	},
	{
		name:                       "HookFunc decides event to be skipped",
		event:                      HookFuncEvent{TestType: "skip"},
		expectedIngestion:          false,
		expectedStreamLoadAttempts: 0,
		expectedResult:             entity.EventProcessingResult{Status: entity.ExecutorStatusSuccessful},
	},
	{
		name:                       "HookFunc decides it want to report back a retryable error",
		event:                      HookFuncEvent{TestType: "retryableError"},
		expectedIngestion:          false,
		expectedStreamLoadAttempts: 0,
		expectedResult:             entity.EventProcessingResult{Status: entity.ExecutorStatusRetriesExhausted, Error: ErrHookRetryableError, Retryable: true},
	},
	{
		name:                       "HookFunc decides it want to report back a unretryable error",
		event:                      HookFuncEvent{TestType: "unretryableError"},
		expectedIngestion:          false,
		expectedStreamLoadAttempts: 0,
		expectedResult:             entity.EventProcessingResult{Status: entity.ExecutorStatusError, Error: ErrHookUnretryableError, Retryable: false},
	},
	{
		name:                       "HookFunc decides the stream should be terminated",
		event:                      HookFuncEvent{TestType: "shutdown"},
		expectedIngestion:          false,
		expectedStreamLoadAttempts: 0,
		expectedResult:             entity.EventProcessingResult{Status: entity.ExecutorStatusShutdown},
	},
	{
		name:                       "HookFunc has a bug and returns invalid action value",
		event:                      HookFuncEvent{TestType: "invalidReturnValue"},
		expectedIngestion:          false,
		expectedStreamLoadAttempts: 0,
		expectedResult:             entity.EventProcessingResult{Status: entity.ExecutorStatusError, Error: fmt.Errorf("%w : %v", ErrHookInvalidAction, 76395)},
	},
	{
		name:                        "Check received stream ID",
		event:                       HookFuncEvent{TestType: "enrichWithStreamId"},
		expectedIngestion:           true,
		expectedEnrichedEventInSink: "{\"TestType\":\"enrichWithStreamId\",\"SomeInputValue\":\"geisttest-minspec\"}",
		expectedStreamLoadAttempts:  1,
		expectedResult:              entity.EventProcessingResult{Status: entity.ExecutorStatusSuccessful, ResourceId: properResourceId},
	},
}

func TestExecutorPreTransformHookLogic(t *testing.T) {

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

	var (
		expectedMetrics entity.Metrics
		bytesIngested   int64
	)
	for _, tc := range HookLogicTestCases {
		t.Run(tc.name, func(t *testing.T) {

			e := newHookEvent(tc.event)
			streamLoadAttempts = 0
			expectedMetrics.Microbatches++
			expectedMetrics.EventsProcessed++
			expectedMetrics.BytesProcessed += int64(len(e[0].Data))
			if tc.expectedIngestion {
				expectedMetrics.EventsStoredInSink++
				expectedMetrics.SinkOperations++
				if tc.expectedEnrichedEventInSink != "" {
					bytesIngested = int64(len(tc.expectedEnrichedEventInSink))
				} else {
					bytesIngested = int64(len(e[0].Data))
				}
				expectedMetrics.BytesIngested = expectedMetrics.BytesIngested + bytesIngested
			}

			result := executor.ProcessEvent(context.Background(), e)
			assert.Equal(t, tc.expectedStreamLoadAttempts, streamLoadAttempts)
			assert.Equal(t, tc.expectedResult, result)
			assertEqualMetrics(t, expectedMetrics, executor.Metrics())

			if tc.expectedIngestion && tc.expectedEnrichedEventInSink != "" {
				eventStoredInSink := loader.Data[0].Data["originalEventData"]
				assert.Equal(t, tc.expectedEnrichedEventInSink, string(eventStoredInSink.([]byte)))
			}
		})
	}
}

func TestExecutorPostTransformHookLogic(t *testing.T) {

	spec, err := entity.NewSpec(specForPostTransformHookTest)
	assert.NoError(t, err)
	transformer := transform.NewTransformer(spec)
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
		PostTransformHookFunc: postTransformHookFunc,
		NotifyChan:            notifyChan,
	}

	executor := NewExecutor(config, stream)
	executor.notifier.SetNotifyLevel(entity.NotifyLevelDebug)
	assert.Equal(t, entity.Metrics{}, executor.Metrics())

	var expectedMetrics entity.Metrics
	for _, tc := range HookLogicTestCases {
		if tc.event.TestType == "enrichWithStreamId" {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {

			e := newHookEvent(tc.event)
			streamLoadAttempts = 0
			expectedMetrics.Microbatches++
			expectedMetrics.EventsProcessed++
			if tc.expectedIngestion {
				expectedMetrics.EventsStoredInSink++
				expectedMetrics.SinkOperations++
			}

			result := executor.ProcessEvent(context.Background(), e)
			assert.Equal(t, tc.expectedStreamLoadAttempts, streamLoadAttempts)
			assert.Equal(t, tc.expectedResult, result)
			assert.Equal(t, expectedMetrics.Microbatches, executor.Metrics().Microbatches)
			assert.Equal(t, expectedMetrics.EventsProcessed, executor.Metrics().EventsProcessed)
			assert.Equal(t, expectedMetrics.EventsStoredInSink, executor.Metrics().EventsStoredInSink)
			assert.Equal(t, expectedMetrics.SinkOperations, executor.Metrics().SinkOperations)

			if tc.expectedIngestion && len(tc.expectedPostHookKeyValue) > 0 {
				enrichedFieldValue := getValueFromTransformed(tc.expectedPostHookKeyValue[0], loader.Data)
				assert.Equal(t, tc.expectedPostHookKeyValue[1], enrichedFieldValue)
			}
		})
	}
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
	case "modifyExistingField":
		e.SomeInputValue += " + myEnrichedValue"
		*event, err = json.Marshal(e)
		if err != nil {
			return entity.HookActionUnretryableError
		}
		return entity.HookActionProceed
	case "injectNewField":
		*event, err = sjson.SetBytes(*event, "myInjectedField", "coolValue")
		if err != nil {
			return entity.HookActionUnretryableError
		}
		return entity.HookActionProceed
	case "skip":
		return entity.HookActionSkip
	case "retryableError":
		return entity.HookActionRetryableError
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

func postTransformHookFunc(ctx context.Context, spec *entity.Spec, event *[]*entity.Transformed) entity.HookAction {

	switch getValueFromTransformed("testType", *event) {
	case "continue":
		return entity.HookActionProceed
	case "modifyExistingField":
		(*event)[0].Data["testType"] = "mutatedTestTypeValue"
		return entity.HookActionProceed
	case "injectNewField":
		(*event)[0].Data["myInjectedField"] = "myInjectedFieldValue"
		return entity.HookActionProceed
	case "skip":
		return entity.HookActionSkip
	case "retryableError":
		return entity.HookActionRetryableError
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

func getValueFromTransformed(key string, transEvent []*entity.Transformed) string {
	for _, kv := range transEvent {
		if value, ok := kv.Data[key]; ok {
			return value.(string) // test func assuming only strings used
		}
	}
	return ""
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
