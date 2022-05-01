package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/internal/pkg/etltest"
	"github.com/zpiroux/geist/internal/pkg/model"
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

func tinyTestEvent(ts time.Time) []model.Event {
	return []model.Event{{Data: tinyEvent, Ts: ts, Key: nil}}
}

var streamLoadAttempts int

func TestExecutor_ProcessEvent(t *testing.T) {

	spec, err := model.NewSpec(minimalStreamSpec)
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
	assert.Equal(t, model.ExecutorStatusSuccessful, result.Status)

	// Test correct result when nothing to transform
	stream.transformer = &MockTransformer_NothingToTransform{}
	executor.stream = stream
	result = executor.ProcessEvent(context.Background(), tinyTestEvent(time.Now()))
	assert.NoError(t, result.Error)
	assert.Equal(t, model.ExecutorStatusSuccessful, result.Status)

	// Test handling of non-retryable error
	stream.loader = &MockLoader_Error{}
	stream.transformer = transformer
	streamLoadAttempts = 0
	executor.stream = stream
	result = executor.ProcessEvent(context.Background(), tinyTestEvent(time.Now()))
	assert.Error(t, result.Error)
	assert.Equal(t, noResourceId, result.ResourceId)
	assert.Equal(t, 1, streamLoadAttempts)
	assert.Equal(t, model.ExecutorStatusError, result.Status)

	// Test handling of retryable error
	stream.loader = &MockLoader_RetryableError{}
	streamLoadAttempts = 0
	executor.stream = stream
	result = executor.ProcessEvent(context.Background(), tinyTestEvent(time.Now()))
	assert.Error(t, result.Error)
	assert.Equal(t, noResourceId, result.ResourceId)
	assert.Equal(t, model.DefaultMaxEventProcessingRetries+1, streamLoadAttempts)
	assert.Equal(t, model.ExecutorStatusRetriesExhausted, result.Status)
}

// Test processing of multiple events in a single call
func TestExecutor_ProcessMultiEvent(t *testing.T) {

	spec, err := model.NewSpec(minimalStreamSpec)
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
	events := []model.Event{{Data: tinyEvent, Ts: ts, Key: []byte("foo")}, {Data: tinyEvent2, Ts: ts, Key: []byte("bar")}}
	result := executor.ProcessEvent(context.Background(), events)
	assert.NoError(t, result.Error)
	assert.Equal(t, properResourceId, result.ResourceId)
	assert.Equal(t, 1, streamLoadAttempts)
	assert.Equal(t, model.ExecutorStatusSuccessful, result.Status)
	assert.Equal(t, tinyEvent, loader.Data[0].Data["originalEventData"])
	assert.Equal(t, tinyEvent2, loader.Data[1].Data["originalEventData"])
}

type MockLoader_StoreLatest struct {
	Data []*model.Transformed
}

func (s *MockLoader_StoreLatest) StreamLoad(ctx context.Context, data []*model.Transformed) (string, error, bool) {
	streamLoadAttempts++
	s.Data = data
	return properResourceId, nil, false
}

func (s *MockLoader_StoreLatest) Shutdown() {}

type MockLoader_NoError struct{}

func (s *MockLoader_NoError) StreamLoad(ctx context.Context, data []*model.Transformed) (string, error, bool) {
	streamLoadAttempts++
	return properResourceId, nil, false
}

func (s *MockLoader_NoError) Shutdown() {}

type MockLoader_Error struct{}

func (s *MockLoader_Error) StreamLoad(ctx context.Context, data []*model.Transformed) (string, error, bool) {
	streamLoadAttempts++
	return noResourceId, errors.New("something bad happened"), false
}

func (s *MockLoader_Error) Shutdown() {}

type MockLoader_RetryableError struct{}

func (s *MockLoader_RetryableError) StreamLoad(ctx context.Context, data []*model.Transformed) (string, error, bool) {
	streamLoadAttempts++
	return noResourceId, errors.New("something bad happened"), true
}

func (s *MockLoader_RetryableError) Shutdown() {}

type MockTransformer struct {
	spec model.Transform
}

func NewMockTransformer(spec model.Transform) *MockTransformer {
	return &MockTransformer{
		spec: spec,
	}
}

func (s *MockTransformer) Transform(
	ctx context.Context,
	event []byte,
	retryable *bool) ([]*model.Transformed, error) {

	transformed := model.NewTransformed()

	transformed.Data["originalEventData"] = event
	transformed.Data["resultFromTransformation"] = "cool transform of '" + string(event) + "'"

	return []*model.Transformed{transformed}, nil
}

type MockTransformer_NothingToTransform struct{}

func (s *MockTransformer_NothingToTransform) Transform(
	ctx context.Context,
	event []byte,
	retryable *bool) ([]*model.Transformed, error) {

	return nil, nil
}
