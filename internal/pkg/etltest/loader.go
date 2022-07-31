package etltest

import (
	"context"

	"github.com/zpiroux/geist/entity"
)

type MockLoader struct {
}

func NewMockLoader() *MockLoader {
	return &MockLoader{}
}

func (s *MockLoader) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {
	return "", nil, false
}

func (s *MockLoader) Shutdown() {
	// Nothing to mock here
}
