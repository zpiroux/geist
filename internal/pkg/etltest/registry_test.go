package etltest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zpiroux/geist/entity"
)

const testDirPath = "../../../test/"

func TestRegistry(t *testing.T) {

	ctx := context.Background()
	registry := NewStreamRegistry(testDirPath)
	assert.NotNil(t, registry)

	err := registry.Fetch(ctx)
	require.NoError(t, err)

	spec, err := registry.Get(ctx, SpecKafkaSrcBigtableSinkPlayer)
	require.NoError(t, err)

	var geistSpec = spec.(*entity.Spec)
	assert.NotZero(t, len(geistSpec.Namespace))
	assert.NotZero(t, len(geistSpec.StreamIdSuffix))

	allSpecs, err := registry.GetAll(ctx)
	require.NoError(t, err)
	assert.NotZero(t, len(allSpecs))

	spec = SpecSpec()
	assert.NotNil(t, spec)

}

func TestAllTestSpecs(t *testing.T) {
	specs := GetAllSpecsRaw(testDirPath)
	assert.NotEmpty(t, specs)
	for _, specData := range specs {
		spec, err := entity.NewSpec(specData)
		assert.NoError(t, err)
		require.NotNil(t, spec)
	}
}
