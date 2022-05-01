package void

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/internal/pkg/entity/transform"
	"github.com/zpiroux/geist/internal/pkg/model"
)

func TestProperties(t *testing.T) {

	retryable := false
	event := []byte("Hi there!")
	ctx := context.Background()
	spec, err := model.NewSpec(specBytes)
	assert.NoError(t, err)
	transformer := transform.NewTransformer(spec)
	transformed, err := transformer.Transform(ctx, event, &retryable)
	assert.NoError(t, err)

	l, err := NewLoader(spec)
	assert.NoError(t, err)

	assert.Equal(t, "true", l.props["logEventData"])

	l.StreamLoad(ctx, transformed)

}

var specBytes = []byte(`{
    "namespace": "my",
    "streamIdSuffix": "tiny-stream",
    "description": "Tiny test stream logging event data to console.",
    "version": 1,
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
 }`)
