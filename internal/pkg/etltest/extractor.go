package etltest

import (
	"context"
	"time"

	"github.com/zpiroux/geist/internal/pkg/model"
)

type MockExtractor struct {
	spec model.SourceConfig
}

func NewMockExtractor(spec model.SourceConfig) *MockExtractor {
	return &MockExtractor{
		spec: spec,
	}
}

func (m *MockExtractor) StreamExtract(
	ctx context.Context,
	reportEvent model.ProcessEventFunc,
	err *error,
	retryable *bool) {

	var event = "Yo!"
	result := reportEvent(ctx, []model.Event{{Data: []byte(event), Ts: time.Now(), Key: []byte("key")}})
	*err = result.Error
	*retryable = result.Retryable
}

func (m *MockExtractor) Extract(ctx context.Context, query model.ExtractorQuery, result any) (error, bool) {
	return nil, false
}

func (m *MockExtractor) ExtractFromSink(ctx context.Context, query model.ExtractorQuery, result *[]*model.Transformed) (error, bool) {
	return nil, false
}

func (m *MockExtractor) SendToSource(ctx context.Context, eventData any) (string, error) {
	return "", nil
}
