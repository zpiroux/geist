package void

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/entity/transform"
)

func TestProperties(t *testing.T) {

	retryable := false
	event := []byte("Hi there!")
	ctx := context.Background()
	spec, err := entity.NewSpec(specBytes)
	assert.NoError(t, err)
	transformer := transform.NewTransformer(spec)
	transformed, err := transformer.Transform(ctx, event, &retryable)
	assert.NoError(t, err)

   lf := NewLoaderFactory()

	l, err := lf.NewLoader(context.Background(), spec, "someId")
	assert.NoError(t, err)
   voidLoader := l.(*loader)

	assert.Equal(t, "true", voidLoader.props["logEventData"])

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
