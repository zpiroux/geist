package eventsim

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetOfStrings(t *testing.T) {

	fieldSpec := []FieldSpec{
		{
			Field: "berryType",
			SetOfStrings: &SetOfStrings{
				Amount: 5,
				Prefix: "unknownBerry",
				ExcludeValues: []string{
					"unknownBerry2",
				},
			},
		},
	}

	expectedOutSpec := []FieldSpec{
		{
			Field: "berryType",
			PredefinedValues: []PredefinedValue{
				{
					Value:           "unknownBerry1",
					FrequencyFactor: 1,
				},
				{
					Value:           "unknownBerry3",
					FrequencyFactor: 1,
				},
				{
					Value:           "unknownBerry4",
					FrequencyFactor: 1,
				},
				{
					Value:           "unknownBerry5",
					FrequencyFactor: 1,
				},
			},
		},
	}

	outSpec := GenerateFieldsFromSetOfStringsSpec(fieldSpec)
	assert.ElementsMatch(t, expectedOutSpec, outSpec)
}
