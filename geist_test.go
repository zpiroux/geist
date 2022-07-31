package geist

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/entity"
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

	ctx := context.Background()
	_, err := New(ctx, &Config{})
	assert.Equal(t, err, ErrConfigNotInitialized)

	//log = log.WithLevel(logger.DEBUG)
	cfg := NewConfig()
	geist, err := New(ctx, cfg)
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
	spec, err := entity.NewSpec(testSpec1)
	assert.NoError(t, err)
	assert.Equal(t, string(spec.JSON()), string(specBytesOut))

	// Validate exposed stream spec
	var xspec entity.Spec
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
	assert.EqualError(t, err, ErrInvalidStreamSpec.Error()+", details: "+" - (root): namespace is required - (root): streamIdSuffix is required - (root): version is required - (root): description is required - (root): source is required - (root): transform is required - (root): sink is required - (root): Additional property spec is not allowed")

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
	assert.Equal(t, err, ErrCodeInvalidSpecRegOp)

	// Test Publish to a non-existent stream
	eventId, err = geist.Publish(ctx, "non-existent stream id", event)
	assert.Empty(t, eventId)
	assert.True(t, errors.Is(err, ErrInvalidStreamId))

	err = geist.Shutdown(ctx)
	assert.NoError(t, err)
}

func TestEnrichment(t *testing.T) {

	var (
		originalEvent         = []byte(`{"field1":"field1Value","field2":11}`)
		expectedEnrichedEvent = []byte(`{"field1":"field1Value","field2":77,"injectedField":"hi there"}`)
	)

	enrichedEvent, err := EnrichEvent(originalEvent, "field2", 77)
	assert.NoError(t, err)
	enrichedEvent, err = EnrichEvent(enrichedEvent, "injectedField", "hi there")
	assert.NoError(t, err)
	assert.Equal(t, string(expectedEnrichedEvent), string(enrichedEvent))
}

// TestPluggableEntities tests advanced Geist usage, where the user of Geist registers and make use of
// some custom-made source/sink plugins, and also adds custom Stream Spec objects, only understandable
// by this custom plugin.
func TestPluggableEntities(t *testing.T) {

	const (
		sourceId = "sillysource"
		sinkId   = "sillysink"
	)
	ctx := context.Background()
	config := NewConfig()

	ef := SillyExtractorFactory{sourceId: sourceId}
	lf := SillyLoaderFactory{sinkId: sinkId}
	err := config.RegisterExtractorType(&ef)
	assert.NoError(t, err)
	err = config.RegisterLoaderType(&lf)
	assert.NoError(t, err)

	geist, err := New(ctx, config)
	assert.NoError(t, err)

	assert.True(t, geist.Entities()["loader"][sinkId])
	assert.True(t, geist.Entities()["extractor"][sourceId])
	assert.False(t, geist.Entities()["loader"]["some_other_sink"])

	go func() {
		streamId, err := geist.RegisterStream(ctx, testSpecPluggable)
		assert.NoError(t, err)
		assert.Equal(t, "geist-test-pluggable", streamId)

		// After stream is regestered/deployed, its Silly Source extractor simply "extracts" a single event,
		// according to custom rules provided in the Sink config in the stream spec,
		// for downstream processing and capturing in-mem by Silly Sink loader, as verified below.

		err = geist.Shutdown(ctx)
		assert.NoError(t, err)
	}()

	err = geist.Run(ctx)
	assert.Equal(t, "{ \"rawEvent\": \"<cool-prefix> HI FROM SILLY SOURCE! (string)\" }", lf.latestLoader.lastEvent)
	assert.NoError(t, err)
}

var testSpecPluggable = []byte(`
{
    "namespace": "geist",
    "streamIdSuffix": "test-pluggable",
    "description": "Simple test spec",
    "version": 1,
    "source": {
        "type": "sillysource",
        "config": {
            "customConfig": {
                "eventToSend": "Hi from silly source!",
                "extraInfo": {
                    "convertToUpper": true,
                    "prefix": "<cool-prefix> "
                }
            }
        }
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
        "type": "sillysink"
    }
}
`)

type SillyExtractorFactory struct {
	sourceId string
}

func (sef *SillyExtractorFactory) SourceId() string {
	return sef.sourceId
}

func (sef *SillyExtractorFactory) NewExtractor(ctx context.Context, spec *entity.Spec, id string) (entity.Extractor, error) {
	return &sillyExtractor{spec: spec}, nil
}

func (sef *SillyExtractorFactory) Close() error {
	return nil
}

type sillyExtractor struct {
	spec *entity.Spec
}

func (se *sillyExtractor) StreamExtract(
	ctx context.Context,
	reportEvent entity.ProcessEventFunc,
	err *error,
	retryable *bool) {

	// This part validates the use of the generic CustomConfig option in the Stream Spec,
	// with which it's possible to send any type of plugin-specific config, not already
	// specified in the Spec schema.
	customConfig := se.spec.Source.Config.CustomConfig.(map[string]any)
	extraInfo := customConfig["extraInfo"].(map[string]any)
	eventStr := customConfig["eventToSend"].(string)
	if extraInfo["convertToUpper"].(bool) {
		eventStr = strings.ToUpper(eventStr)
	}
	eventStr = extraInfo["prefix"].(string) + eventStr

	result := reportEvent(ctx, []entity.Event{{Data: []byte(eventStr)}})
	*err = result.Error
	*retryable = result.Retryable
}

func (se *sillyExtractor) Extract(ctx context.Context, query entity.ExtractorQuery, result any) (error, bool) {
	return nil, false
}
func (se *sillyExtractor) ExtractFromSink(ctx context.Context, query entity.ExtractorQuery, result *[]*entity.Transformed) (error, bool) {
	return nil, false
}
func (se *sillyExtractor) SendToSource(ctx context.Context, event any) (string, error) {
	return "silly-resource-id", nil
}

type SillyLoaderFactory struct {
	sinkId       string
	latestLoader *sillyLoader
}

func (slf *SillyLoaderFactory) SinkId() string {
	return slf.sinkId
}

func (slf *SillyLoaderFactory) NewLoader(ctx context.Context, spec *entity.Spec, id string) (entity.Loader, error) {
	slf.latestLoader = &sillyLoader{}
	return slf.latestLoader, nil
}
func (slf *SillyLoaderFactory) NewSinkExtractor(ctx context.Context, spec *entity.Spec, id string) (entity.Extractor, error) {
	return nil, nil
}
func (slf *SillyLoaderFactory) Close() error {
	return nil
}

type sillyLoader struct {
	lastEvent string
}

func (sl *sillyLoader) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {
	sl.lastEvent = data[0].String()
	return "resource id based on " + sl.lastEvent, nil, false
}

func (sl *sillyLoader) Shutdown() {
	// nothing to mock here
}
