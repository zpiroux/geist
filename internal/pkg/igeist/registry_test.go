package igeist

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

type SpecRegistryTest struct {
	specs map[string]Spec
}

type TestSpec struct {
	Data string
}

func (t TestSpec) Id() string       { return "" }
func (t TestSpec) IsDisabled() bool { return false }
func (t TestSpec) Validate() error  { return nil }
func (t TestSpec) JSON() []byte {
	data, _ := json.Marshal(t)
	return data
}

func NewSpecRegistry() *SpecRegistryTest {
	var r SpecRegistryTest
	r.specs = make(map[string]Spec)
	return &r
}

func (s *SpecRegistryTest) Put(ctx context.Context, id string, spec Spec) error {
	s.specs[id] = spec
	return nil
}

func (s *SpecRegistryTest) Fetch(ctx context.Context) error {
	return nil
}

func (s *SpecRegistryTest) Get(ctx context.Context, id string) (Spec, error) {
	return s.specs[id], nil
}

func (s *SpecRegistryTest) GetAll(ctx context.Context) (map[string]Spec, error) {
	return s.specs, nil
}

func (s *SpecRegistryTest) Delete(ctx context.Context, id string) error {
	return nil
}

func (s *SpecRegistryTest) Exists(id string) bool {
	return false
}

func (s *SpecRegistryTest) ExistsSameVersion(specData []byte) (bool, error) {
	return false, nil
}

func (s *SpecRegistryTest) Validate(spec []byte) (Spec, error) {
	return nil, nil
}

type Boss struct {
	registry Registry
}

func TestSpecRegistry(t *testing.T) {

	var (
		boss         Boss
		registryImpl = NewSpecRegistry()
		spec         TestSpec
		specData     = "Hiya!"
		ctx          = context.Background()
	)

	spec.Data = specData
	boss.registry = registryImpl
	_ = boss.registry.Put(ctx, "coolId", spec)
	returnedSpec, _ := boss.registry.Get(ctx, "coolId")
	assert.Equal(t, returnedSpec.(TestSpec).Data, specData)
}
