package etltest

import (
	"context"
	"time"

	"github.com/zpiroux/geist/entity"
)

type MockExtractor struct {
	spec entity.SourceConfig
}

func NewMockExtractor(spec entity.SourceConfig) *MockExtractor {
	return &MockExtractor{
		spec: spec,
	}
}

func (m *MockExtractor) StreamExtract(
	ctx context.Context,
	reportEvent entity.ProcessEventFunc,
	err *error,
	retryable *bool) {

	var event = "Yo!"
	result := reportEvent(ctx, []entity.Event{{Data: []byte(event), Ts: time.Now(), Key: []byte("key")}})
	*err = result.Error
	*retryable = result.Retryable
}

func (m *MockExtractor) Extract(ctx context.Context, query entity.ExtractorQuery, result any) (error, bool) {
	return nil, false
}

func (m *MockExtractor) ExtractFromSink(ctx context.Context, query entity.ExtractorQuery, result *[]*entity.Transformed) (error, bool) {
	return nil, false
}

func (m *MockExtractor) SendToSource(ctx context.Context, eventData any) (string, error) {
	return "", nil
}
