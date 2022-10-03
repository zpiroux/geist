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

	l, err := lf.NewLoader(context.Background(), entity.Config{Spec: spec, ID: "someId"})
	assert.NoError(t, err)
	voidLoader := l.(*loader)

	assert.Equal(t, "true", voidLoader.props["logEventData"])

	_, err, _ = l.StreamLoad(ctx, transformed)
	assert.NoError(t, err)

	// Test handling of sink mode
	prop := entity.Property{Key: "mode", Value: inMemRegistryMode}
	spec.Sink.Config.Properties = append(spec.Sink.Config.Properties, prop)
	l, err = lf.NewLoader(context.Background(), entity.Config{Spec: spec, ID: "someId"})
	assert.NoError(t, err)
	transformed[0].Data["rawEvent"] = 3
	_, err, _ = l.StreamLoad(ctx, transformed)
	assert.EqualError(t, err, "rawEvent data not found or invalid type", err)

	var emptyTransformed entity.Transformed
	_, err, _ = l.StreamLoad(ctx, []*entity.Transformed{&emptyTransformed})
	assert.EqualError(t, err, "rawEvent data not found or invalid type", err)

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
