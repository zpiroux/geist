package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSpecs(t *testing.T) {
	spec, err := NewSpec(SpecRegistrationSpec)
	assert.NoError(t, err)
	err = spec.Validate()
	assert.NoError(t, err)
}
