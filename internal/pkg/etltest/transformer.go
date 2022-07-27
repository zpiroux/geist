package etltest

import (
	"context"

	"github.com/zpiroux/geist/entity"
)

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
