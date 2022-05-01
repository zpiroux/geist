package etltest

import (
	"context"

	"github.com/zpiroux/geist/internal/pkg/model"
)

type MockLoader struct {
}

func NewMockLoader() *MockLoader {
	return &MockLoader{}
}

func (s *MockLoader) StreamLoad(ctx context.Context, data []*model.Transformed) (string, error, bool) {
	return "", nil, false
}

func (s *MockLoader) Shutdown() {}
