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

	fileBytes, err := os.ReadFile("../test/specs/kafkasrc-bigtablesink-multitable-session.json")
	assert.NoError(t, err)
	spec, err := NewSpec(fileBytes)
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
	specBytes, _ := json.Marshal(spec)
	err = validateRawJson(specBytes)
	assert.NoError(t, err)

	// Custom environment
	topicSpecJSON := []byte(`
  {
    "env": "my-cool-env",
    "names": ["topic1", "topic2"]
  }`)
	var topicSpec Topics
	err = json.Unmarshal(topicSpecJSON, &topicSpec)
	assert.NoError(t, err)
	assert.Equal(t, "my-cool-env", string(topicSpec.Env))

	// Validate regexp compile
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

	// TODO: Add test for timeconv
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
            "id": "rawEvent",
            "type": "string"
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
)
