package geist

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/internal/pkg/model"
)

var testSpec1 = []byte(`
{
   "namespace": "geist",
   "streamIdSuffix": "test1",
   "description": "Simple test spec",
   "version": 1,
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

var testSpec2 = []byte(`
{
   "namespace": "geist",
   "streamIdSuffix": "test2",
   "description": "Simple test spec",
   "version": 1,
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

func TestGeist(t *testing.T) {
	//log = log.WithLevel(logger.DEBUG)
	ctx := context.Background()

	geist, err := New(ctx, Config{})
	assert.NoError(t, err)

	go geistTest(ctx, geist, t)

	err = geist.Run(ctx)
	assert.NoError(t, err)
}

func geistTest(ctx context.Context, geist *Geist, t *testing.T) {

	var id1, id2, eventId string

	// Test register invalid spec
	_, err := geist.RegisterStream(ctx, []byte("hi"))
	assert.Error(t, err)

	// Test register valid specs
	id1, err = geist.RegisterStream(ctx, testSpec1)
	assert.NoError(t, err)
	assert.Equal(t, "geist-test1", id1)
	time.Sleep(2 * time.Second)

	id2, err = geist.RegisterStream(ctx, testSpec2)
	assert.NoError(t, err)
	assert.Equal(t, "geist-test2", id2)
	time.Sleep(2 * time.Second)

	// Test retrieving specs
	specs, err := geist.GetStreamSpecs(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(specs))

	specBytesOut, err := geist.GetStreamSpec("geist-test1")
	assert.NoError(t, err)
	spec, err := model.NewSpec(testSpec1)
	assert.NoError(t, err)
	assert.Equal(t, string(spec.JSON()), string(specBytesOut))

	// Validate exposed stream spec
	var xspec StreamSpec
	err = json.Unmarshal(specBytesOut, &xspec)
	assert.NoError(t, err)
	assert.Equal(t, spec.Source, xspec.Source)
	assert.Equal(t, spec.Transform, xspec.Transform)
	assert.Equal(t, spec.Sink, xspec.Sink)

	// Validate proper spec
	specId, err := geist.ValidateStreamSpec(testSpec2)
	assert.NoError(t, err)
	assert.Equal(t, "geist-test2", specId)

	// Validate incorrect spec
	specId, err = geist.ValidateStreamSpec([]byte(`{ "spec": "nope, not a valid spec"}`))
	assert.Empty(t, specId)
	assert.EqualError(t, err, ErrInvalidStreamSpec+", details: "+" - (root): namespace is required - (root): streamIdSuffix is required - (root): version is required - (root): description is required - (root): source is required - (root): transform is required - (root): sink is required - (root): Additional property spec is not allowed")

	// Test normal Publish
	var event = []byte(`{ "name": "my cool event"}`)
	eventId, err = geist.Publish(ctx, id1, event)
	assert.NoError(t, err)
	fmt.Printf("eventId: %s\n", eventId)
	eventId, err = geist.Publish(ctx, id2, event)
	assert.Equal(t, "<noResourceId>", eventId)
	assert.NoError(t, err)

	// Test Publish directly on to Registry stream not allowed
	regStreamId := "geist-specs"
	eventId, err = geist.Publish(ctx, regStreamId, event)
	assert.Empty(t, eventId)
	assert.EqualError(t, err, ErrCodeInvalidSpecRegOp)

	err = geist.Shutdown(ctx)
	assert.NoError(t, err)
}
