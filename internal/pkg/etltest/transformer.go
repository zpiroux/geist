package etltest

import (
	"context"

	"github.com/zpiroux/geist/internal/pkg/model"
)

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
