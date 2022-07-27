package assembly

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/entity"
)

func TestEntities(t *testing.T) {
	var sef StreamEntityFactory
	sef.config.Extractors = make(entity.ExtractorFactories)
	sef.config.Loaders = make(entity.LoaderFactories)

	sef.config.Extractors["source1"] = nil
	sef.config.Extractors["source2"] = nil
	sef.config.Loaders["sink1"] = nil
	sef.config.Loaders["sink2"] = nil

	entities := sef.Entities()

	assert.True(t, entities["extractor"]["source1"])
	assert.True(t, entities["extractor"]["source1"])
	assert.True(t, entities["loader"]["sink1"])
	assert.True(t, entities["loader"]["sink2"])
	assert.False(t, entities["loader"]["sink3"])
}
