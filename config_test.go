package geist

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/entity"
)

func TestToInternalConfig(t *testing.T) {
	c := NewConfig()
	internalCfg := preProcessConfig(c)
	assert.Equal(t, defaultEventLogInterval, internalCfg.Engine.EventLogInterval)
}

func TestProtectedEntityNames(t *testing.T) {

	c := NewConfig()
	ef := &SillyExtractorFactory{sourceId: "sillysource"}
	lf := &SillyLoaderFactory{sinkId: "sillysink"}
	err := c.RegisterExtractorType(ef)
	assert.NoError(t, err)
	err = c.RegisterLoaderType(lf)
	assert.NoError(t, err)

	for entityName := range entity.ReservedEntityNames {
		lf.sinkId, ef.sourceId = entityName, entityName
		err = c.RegisterExtractorType(ef)
		assert.Equal(t, err, ErrInvalidEntityId)
		err = c.RegisterLoaderType(lf)
		assert.Equal(t, err, ErrInvalidEntityId)
	}
}