package admin

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/entity"
)

func TestSpecs(t *testing.T) {
	spec, err := entity.NewSpec(SpecRegistrationSpec)
	assert.NoError(t, err)
	err = spec.Validate()
	assert.NoError(t, err)
}
