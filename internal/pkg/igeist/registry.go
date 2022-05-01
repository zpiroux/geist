package igeist

import "context"

type Registry interface {
	Fetch(ctx context.Context) error
	Put(ctx context.Context, id string, spec Spec) error
	Get(ctx context.Context, id string) (Spec, error)
	GetAll(ctx context.Context) (map[string]Spec, error)
	Delete(ctx context.Context, id string) error
	Exists(id string) bool
	ExistsSameVersion(specData []byte) (bool, error)
	Validate(spec []byte) (Spec, error)
}
