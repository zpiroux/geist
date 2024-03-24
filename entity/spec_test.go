package entity

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests of transformations using the spec constructs are found in the transform package.
// Tests of various other parts of the spec model, including using all test specs, are found in
// entities using them or in etltest.

func TestSpecModel(t *testing.T) {

	// Validate default values
	spec := NewEmptySpec()
	assert.Equal(t, spec.Ops.StreamsPerPod, DefaultStreamsPerPod)
	assert.Equal(t, spec.Ops.MicroBatch, false)
	assert.Equal(t, spec.Ops.MaxEventProcessingRetries, DefaultMaxEventProcessingRetries)
	assert.Equal(t, spec.Ops.MaxStreamRetryBackoffIntervalSec, DefaultMaxStreamRetryBackoffIntervalSec)
	assert.Equal(t, spec.Ops.HandlingOfUnretryableEvents, HoueDefault)

	// Validate complex spec parsing
	fileBytes, err := os.ReadFile("../test/specs/kafkasrc-bigtablesink-multitable-session.json")
	assert.NoError(t, err)
	spec, err = NewSpec(fileBytes)
	assert.NoError(t, err)
	require.NotNil(t, spec)

	err = spec.Validate()
	assert.NoError(t, err)

	fileBytes, err = os.ReadFile("../test/specs/pubsubsrc-kafkasink-foologs.json")
	assert.NoError(t, err)
	spec, err = NewSpec(fileBytes)
	assert.NoError(t, err)
	require.NotNil(t, spec)

	err = spec.Validate()
	assert.NoError(t, err)

	err = spec.Validate()
	assert.NoError(t, err)

	// Validate auto-correcting defaults
	spec, err = NewSpec(specCorrectableOps)
	require.NoError(t, err)
	assert.Equal(t, DefaultStreamsPerPod, spec.Ops.StreamsPerPod)

	// Raw JSON validation
	err = validateRawJson(specOk)
	assert.NoError(t, err)

	// Missing Sink
	err = validateRawJson(specMissingSink)
	assert.Error(t, err)

	// Empty namespace
	err = validateRawJson(specEmptyNamespace)
	assert.Error(t, err)

	// Spec changes vs JSON schema
	spec = NewEmptySpec()
	spec.Namespace = "foo"
	spec.StreamIdSuffix = "bar"
	spec.Description = "bla bla"
	spec.Source.Type = "kafka"
	spec.Sink.Type = "coolSink"
	specBytes, err := json.Marshal(spec)
	assert.NoError(t, err)
	err = validateRawJson(specBytes)
	assert.NoError(t, err)

	// Validate regexp compile
	// TODO: Add test for timeconv
	fileBytes, err = os.ReadFile("../test/specs/pubsubsrc-regexp-reqs-voidsink.json")
	assert.NoError(t, err)
	spec, err = NewSpec(fileBytes)
	require.NotNil(t, spec)
	assert.NoError(t, err)
	spec.Transform.Regexp.Expression = "this should error,,,"
	assert.EqualError(t, spec.Validate(), "no groupings where found in regular expression this should error,,,")

	spec.Transform.Regexp.Expression = "^.*"
	err = spec.Validate()
	assert.EqualError(t, err, "no groupings where found in regular expression ^.*")

	spec.Transform.Regexp.Expression = "^(?P<testGroup>)"
	err = spec.Validate()
	assert.NoError(t, err)

	// Validate custom props
	spec, err = NewSpec(specCustomProps)
	require.NoError(t, err)
	require.NotNil(t, spec)

}

func TestOpsPerEnv(t *testing.T) {
	const (
		dev     = "dev"
		staging = "staging"
		prod1   = "prod1"
		prod2   = "prod2"
	)

	expectedOps := map[string]Ops{
		dev: {
			StreamsPerPod:                    4,
			MaxEventProcessingRetries:        DefaultMaxEventProcessingRetries,
			MaxStreamRetryBackoffIntervalSec: DefaultMaxStreamRetryBackoffIntervalSec,
			HandlingOfUnretryableEvents:      HoueDefault,
		},
		staging: {
			StreamsPerPod:                    8,
			MaxEventProcessingRetries:        DefaultMaxEventProcessingRetries,
			MaxStreamRetryBackoffIntervalSec: DefaultMaxStreamRetryBackoffIntervalSec,
			HandlingOfUnretryableEvents:      HoueDefault,
		},
		prod1: {
			StreamsPerPod:                    16,
			MaxEventProcessingRetries:        DefaultMaxEventProcessingRetries,
			MaxStreamRetryBackoffIntervalSec: DefaultMaxStreamRetryBackoffIntervalSec,
			HandlingOfUnretryableEvents:      HoueDefault,
		},
		prod2: {
			StreamsPerPod:                    32,
			MaxEventProcessingRetries:        DefaultMaxEventProcessingRetries,
			MaxStreamRetryBackoffIntervalSec: DefaultMaxStreamRetryBackoffIntervalSec,
			HandlingOfUnretryableEvents:      HoueDefault,
		},
	}

	spec, err := NewSpec(specWithOpsPerEnv)
	if assert.NoError(t, err) {
		assert.Equal(t, expectedOps[dev], spec.OpsPerEnv[dev])
		assert.Equal(t, expectedOps[staging], spec.OpsPerEnv[staging])
		assert.Equal(t, expectedOps[prod1], spec.OpsPerEnv[prod1])
		assert.Equal(t, expectedOps[prod2], spec.OpsPerEnv[prod2])
	}

	nonExistingOps, ok := spec.OpsPerEnv["some-invalid-env"]
	assert.Equal(t, Ops{}, nonExistingOps)
	assert.False(t, ok)

	spec, err = NewSpec(specOk)
	if assert.NoError(t, err) {
		assert.Nil(t, spec.OpsPerEnv)
	}

	_, err = NewSpec(specWithInvalidOpsPerEnv)
	assert.EqualError(t, err, " - opsPerEnv: Must validate at least one schema (anyOf) - opsPerEnv.some-env: Additional property foo is not allowed")
}

var (
	specOk = []byte(`
{
  "namespace": "geisttest",
  "streamIdSuffix": "eventlogstream-1",
  "version": 27,
  "description": "A spec for a minimal stream, using GEIST API as source",
  "source": {
    "type": "geistapi"
  },
  "transform": {
    "extractFields": [
      {
        "fields": [
          {
            "id": "rawEvent",
            "type": "string"
          }
        ]
      }
    ]
  },
  "sink": {
    "type": "void"
  }
}
`)

	specWithOpsPerEnv = []byte(`
{
  "namespace": "geisttest",
  "streamIdSuffix": "eventlogstream-1",
  "version": 27,
  "description": "A spec for a minimal stream, using GEIST API as source",
  "opsPerEnv": {
    "dev": {
      "streamsPerPod": 4
    },
    "staging": {
      "streamsPerPod": 8
    },
    "prod1": {
      "streamsPerPod": 16
    },
    "prod2": {
      "streamsPerPod": 32
    }
  },
  "source": {
    "type": "geistapi"
  },
  "transform": {
    "extractFields": [
      {
        "fields": [
          {
            "id": "rawEvent",
            "type": "string"
          }
        ]
      }
    ]
  },
  "sink": {
    "type": "void"
  }
}
`)

	specWithInvalidOpsPerEnv = []byte(`
{
  "namespace": "geisttest",
  "streamIdSuffix": "eventlogstream-1",
  "version": 27,
  "description": "A spec for a minimal stream, using GEIST API as source",
  "opsPerEnv": {
    "some-env": {
      "foo": 4
    }
  },
  "source": {
    "type": "geistapi"
  },
  "transform": {
    "extractFields": [
      {
        "fields": [
          {
            "id": "rawEvent",
            "type": "string"
          }
        ]
      }
    ]
  },
  "sink": {
    "type": "void"
  }
}
`)

	specMissingSink = []byte(`
{
  "namespace": "geisttest",
  "streamIdSuffix": "eventlogstream-1",
  "version": 27,
  "description": "A spec for a minimal stream, using GEIST API as source",
  "source": {
    "type": "geistapi"
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
  }
}
`)
	specEmptyNamespace = []byte(`
{
  "namespace": "",
  "streamIdSuffix": "eventlogstream-1",
  "version": 27,
  "description": "A spec for a minimal stream, using GEIST API as source",
  "source": {
    "type": "geistapi"
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
    "type": "void"
  }
}
`)

	specCorrectableOps = []byte(`
{
  "namespace": "geisttest",
  "streamIdSuffix": "eventlogstream-1",
  "version": 1,
  "description": "A spec for a minimal stream, using GEIST API as source",
  "ops": {
    "streamsPerPod": -1
  },
  "source": {
    "type": "geistapi"
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
    "type": "void"
  }
}
`)

	specCustomProps = []byte(`
{
  "namespace": "geisttest",
  "streamIdSuffix": "eventlogstream-1",
  "version": 1,
  "description": "A spec for a minimal stream, using GEIST API as source",
  "ops": {
    "customProperties": {
      "prop1": "prop1Value",
      "prop2": "prop2Value"
    }
  },
  "source": {
    "type": "geistapi"
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
    "type": "void"
  }
}
`)
)
