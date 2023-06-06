package eventsim

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zpiroux/geist/entity"
)

const (
	MaxSimTriggers     = 4
	PrintCreatedEvents = false
)

func TestSimpleUsage(t *testing.T) {
	var (
		err       error
		retryable bool
	)
	ctx := context.Background()
	eventSim := newEventSimForTesting(t, allOptionsEventSimStreamSpec, nil)
	eventSim.StreamExtract(
		ctx,
		reportEvent,
		&err,
		&retryable)

	require.NoError(t, err)
}

func TestEventGeneration(t *testing.T) {
	runEventGenTests(t)
}

type TestCase struct {
	name                       string
	field                      string
	predefValues               []PredefinedValue
	randValue                  *RandomizedValue
	expectedEvent              []byte
	expectedValLength          *ValueLength
	expectedBool               bool
	expectedNumberString       bool
	expectedTimestampIsoMillis bool
	expectedTimestampIsoMicros bool
}

var testCases = []TestCase{
	{
		name:          "Predefined string",
		field:         "country",
		predefValues:  []PredefinedValue{{Value: "not empty"}},
		expectedEvent: []byte(`{"country":"Nauru"}`),
	},
	{
		name:              "Random string",
		field:             "randomString",
		randValue:         &RandomizedValue{Type: "string", Min: 3, Max: 3},
		expectedValLength: &ValueLength{Min: 3, Max: 3},
	},
	{
		name:                 "Random string from custom charset with only numbers",
		field:                "randomNumberString",
		randValue:            &RandomizedValue{Type: "string", Charset: "myNumberCharset", Min: 4, Max: 7},
		expectedNumberString: true,
	},
	{
		name:              "Random int",
		field:             "randomInt",
		randValue:         &RandomizedValue{Type: "int", Min: 5, Max: 5},
		expectedValLength: &ValueLength{Min: 5, Max: 5},
	},
	{
		name:              "Random float",
		field:             "randomFloat",
		randValue:         &RandomizedValue{Type: "float", Min: 8, Max: 8},
		expectedValLength: &ValueLength{Min: 8, Max: 8},
	},
	{
		name:         "Random bool",
		field:        "randomBool",
		randValue:    &RandomizedValue{Type: "bool"},
		expectedBool: true,
	},
	{
		name:                       "Random iso timestamp (millis)",
		field:                      "randomIsoTimestamp",
		randValue:                  &RandomizedValue{Type: "isoTimestampMilliseconds", JitterMilliseconds: 100},
		expectedTimestampIsoMillis: true,
	},
	{
		name:                       "Random iso timestamp (micros)",
		field:                      "randomIsoTimestamp",
		randValue:                  &RandomizedValue{Type: "isoTimestampMicroseconds", JitterMilliseconds: 300},
		expectedTimestampIsoMicros: true,
	},
	{
		name:              "UUID",
		field:             "uuid",
		randValue:         &RandomizedValue{Type: "uuid"},
		expectedValLength: &ValueLength{Min: 36, Max: 36},
	},
}

type ValueLength struct {
	Min int
	Max int
}

func runEventGenTests(t *testing.T) {
	customCharsets := map[string][]rune{
		"myNumberCharset":  []rune("0123456789"),
		"someOtherCharset": []rune(")(/#&¤=<!"),
	}
	eventSim := newEventSimForTesting(t, eventSimSpecForGenValidation, customCharsets)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testPredefValues(t, tc, eventSim)
			testRandValues(t, tc, eventSim)
		})
	}
}

func testPredefValues(t *testing.T, tc TestCase, eventSim *eventSim) {
	if tc.predefValues == nil {
		return
	}
	fieldToGenerate := FieldSpec{
		Field:            tc.field,
		PredefinedValues: []PredefinedValue{{Value: "pre-prepped at init"}},
		RandomizedValue:  tc.randValue,
	}
	event, err := eventSim.createEvent(EventSpec{Fields: []FieldSpec{fieldToGenerate}})
	assert.NoError(t, err)
	assert.Equal(t, tc.expectedEvent, event, string(event))
}

func testRandValues(t *testing.T, tc TestCase, eventSim *eventSim) {
	if tc.randValue == nil {
		return
	}

	fieldToGenerate := FieldSpec{
		Field:           tc.field,
		RandomizedValue: tc.randValue,
	}
	value, err := eventSim.createRandomizedFieldValue(fieldToGenerate)
	assert.NoError(t, err)

	switch {
	case tc.expectedValLength != nil:
		len := lenSpecial(value)
		assert.True(t, len >= tc.expectedValLength.Min && len <= tc.expectedValLength.Max, "value: %#v, len: %d", value, len)

	case tc.expectedTimestampIsoMillis:
		_, err := time.Parse(TimestampLayoutIsoMillis, value.(string))
		assert.NoError(t, err)

	case tc.expectedTimestampIsoMicros:
		_, err := time.Parse(TimestampLayoutIsoMicros, value.(string))
		assert.NoError(t, err)

	case tc.expectedNumberString:
		_, err := strconv.Atoi(value.(string))
		assert.NoError(t, err)

	case tc.expectedBool:
		_ = value.(bool)
	}
}

func lenSpecial(value any) int {
	switch v := value.(type) {
	case string:
		return len(v)
	case int:
		return v
	case json.Number:
		n, err := v.Float64()
		if err != nil {
			log.Fatal("failed converting json.Number to float64")
		}
		return int(n)
	default:
		log.Fatalf("currently unsupported type in lenAny: %#v", v)
	}
	return 0
}

func newEventSimForTesting(t *testing.T, specBytes []byte, customCharsets map[string][]rune) *eventSim {
	spec, err := entity.NewSpec(specBytes)
	require.NoError(t, err)
	eventSim, err := newEventSim(entity.Config{Spec: spec, ID: "someInstanceID", Log: true}, customCharsets)
	require.NoError(t, err)
	return eventSim
}

var nbReportEventCalls int

func reportEvent(ctx context.Context, events []entity.Event) entity.EventProcessingResult {
	if nbReportEventCalls < MaxSimTriggers {
		if PrintCreatedEvents {
			fmt.Printf("Generated %d events: %+v\n", len(events), events)
		}
		nbReportEventCalls++
		return entity.EventProcessingResult{Status: entity.ExecutorStatusSuccessful, ResourceId: "no-resource-id"}
	} else {
		return entity.EventProcessingResult{Status: entity.ExecutorStatusShutdown, ResourceId: "no-resource-id"}
	}
}

var allOptionsEventSimStreamSpec = []byte(`
{
    "namespace": "my",
    "streamIdSuffix": "event-sim-stream",
    "description": "Simple event sim stream for testing",
    "version": 1,
    "source": {
        "type": "eventsim",
        "config": {
            "customConfig": {
                "simResolutionMilliseconds": 2000,
                "eventGeneration": {
                    "type": "sinusoid",
                    "minCount": 1,
                    "maxCount": 10,
                    "periodSeconds": 60,
                    "peakTime": "2024-04-25T16:04:00Z"
                },
                "eventSpec": {
                    "fields": [
                        {
                            "field": "foo.bar",
                            "predefinedValues": [
                                {
                                    "value": "str1"
                                },
                                {
                                    "value": "str2"
                                },
                                {
                                    "value": "str3"
                                }
                            ]
                        },
                        {
                            "field": "foo.berry",
                            "predefinedValues": [
                                {
                                    "value": "blueberry",
                                    "frequencyFactor": 60
                                },
                                {
                                    "value": "blackberry",
                                    "frequencyFactor": 30
                                },
                                {
                                    "value": "cloudberry",
                                    "frequencyFactor": 10
                                }
                            ]
                        },
                        {
                            "field": "foo.baz",
                            "predefinedValues": [
                                {
                                    "value": 41,
                                    "frequencyFactor": 137
                                },
                                {
                                    "value": 42,
                                    "frequencyFactor": 205
                                },
                                {
                                    "value": 43,
                                    "frequencyFactor": 52
                                }
                            ]
                        },
                        {
                            "field": "foo.someFlag",
                            "predefinedValues": [
                                {
                                    "value": true,
                                    "frequencyFactor": 60
                                },
                                {
                                    "value": false,
                                    "frequencyFactor": 40
                                }
                            ]
                        },
                        {
                            "field": "foo.someMissingValue",
                            "predefinedValues": [
                                {
                                    "value": null
                                }
                            ]
                        },
                        {
                            "field": "foo.someIntMetric",
                            "randomizedValue": {
                                "type": "int",
                                "min": -3,
                                "max": 4
                            }
                        },
                        {
                            "field": "foo.someFloatMetric",
                            "randomizedValue": {
                                "type": "float",
                                "min": 0,
                                "max": 100,
                                "maxFractionDigits": 3
                            }
                        },
                        {
                            "field": "foo.someString",
                            "randomizedValue": {
                                "type": "string",
                                "min": 4,
                                "max": 9
                            }
                        },
                        {
                            "field": "foo.someStringWithNumbers",
                            "randomizedValue": {
                                "type": "string",
                                "charset": "myNumberCharset",
                                "min": 5,
                                "max": 6
                            }
                        },
                        {
                            "field": "foo.someBool",
                            "randomizedValue": {
                                "type": "bool"
                            }
                        },
                        {
                            "field": "foo.someIsoTimestampMillis",
                            "randomizedValue": {
                                "type": "isoTimestampMilliseconds",
                                "jitterMilliseconds": 2000
                            }
                        },
                        {
                            "field": "foo.someIsoTimestampMicros",
                            "randomizedValue": {
                                "type": "isoTimestampMicroseconds"
                            }
                        },
                        {
                            "field": "foo.someUUID",
                            "randomizedValue": {
                                "type": "uuid"
                            }
                        }
                    ]
                }
            }
        }
    },
    "transform": {
        "extractFields": [
            {
                "fields": [
                    {
                        "id": "rawEvent"
                    },
                    {
                        "id": "eventTime",
                        "jsonPath": "foo.someIsoTimestampMillis",
                        "type": "isoTimestamp"
                    }
                ]
            }
        ]
    },
    "sink": {
        "type": "void",
        "config": {
            "properties": [
                {
                    "key": "logEventData",
                    "value": "true"
                }
            ]
        }
    }
}
`)

var eventSimSpecForGenValidation = []byte(`
{
    "namespace": "my",
    "streamIdSuffix": "event-sim-stream",
    "description": "Minium spec for event generation validation",
    "version": 1,
    "source": {
        "type": "eventsim",
        "config": {
            "customConfig": {
                "eventSpec": {
                    "fields": [
                        {
                            "field": "country",
                            "predefinedValues": [
                                {
                                    "value": "Nauru"
                                }
                            ]
                        }
                    ]
                }
            }
        }
    },
    "transform": {
        "extractFields": [
            {
                "fields": [
                    {
                        "id": "rawEvent"
                    }
                ]
            }
        ]
    },
    "sink": {
        "type": "void",
        "config": {}
    }
}
`)
