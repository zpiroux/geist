package igeist

import (
	"context"

	"github.com/zpiroux/geist/entity"
)

type Registry interface {
	Fetch(ctx context.Context) error
	Put(ctx context.Context, id string, spec *entity.Spec) error
	Get(ctx context.Context, id string) (*entity.Spec, error)
	GetAll(ctx context.Context) (map[string]*entity.Spec, error)
	Delete(ctx context.Context, id string) error
	Exists(id string) bool
	ExistsWithSameOrHigherVersion(specData []byte) (bool, error)
	Validate(spec []byte) (*entity.Spec, error)
}
