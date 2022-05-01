package channel

import (
	"context"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/internal/pkg/model"
)

const (
	testDirPath  = "../../../../test/"
	testSpecDir  = testDirPath + "specs/"
	testEventDir = testDirPath + "events/"
)

var (
	SpecRegSpec               = model.SpecRegistrationSpec
	SpecRegChangeNotification = model.AdminEventSpec
)

var printTestOutput bool

func TestNewExtractor(t *testing.T) {

	printTestOutput = false

	tPrintf("Starting TestNewExtractor\n")

	extractor := newTestExtractor(t, SpecRegSpec)
	assert.NotNil(t, extractor)

	extractor = newTestExtractor(t, SpecRegChangeNotification)
	assert.NotNil(t, extractor)

	extractor = newTestExtractorFromFile(t, "apisrc-bigtablesink-fooround.json")
	assert.NotNil(t, extractor)
}

func TestExtractor_StreamExtract(t *testing.T) {

	var (
		err       error
		retryable bool
	)
	ctx := context.Background()

	tPrintf("Starting TestExtractor_StreamExtract\n")

	extractor := newTestExtractor(t, SpecRegSpec)
	assert.NotNil(t, extractor)

	go extractor.StreamExtract(
		ctx,
		reportEvent,
		&err,
		&retryable)

	id, err := extractor.SendToSource(context.Background(), getTestEvent("geist_spec_minimal_api_void.json"))
	assert.NoError(t, err)
	assert.NotEmpty(t, id)
}

func reportEvent(ctx context.Context, events []model.Event) model.EventProcessingResult {

	tPrintf("In reportEvent in Mock Executor, events: %+v\n", events)

	return model.EventProcessingResult{ResourceId: "no-resource-id"}
}

func newTestExtractor(t *testing.T, specBytes []byte) *Extractor {

	spec, err := model.NewSpec(specBytes)
	assert.NoError(t, err)

	extractor, err := NewExtractor(spec.Id(), "instanceId")
	assert.NoError(t, err)
	assert.NotNil(t, extractor)

	return extractor
}


func newTestExtractorFromFile(t *testing.T, specId string) *Extractor {

	spec, err := getTestSpec(specId)
	assert.NoError(t, err)
	assert.NotNil(t, spec)

	extractor, err := NewExtractor(spec.Id(), "instanceId")
	assert.NoError(t, err)
	assert.NotNil(t, extractor)

	return extractor
}

func getTestSpec(specFile string) (*model.Spec, error) {
	tPrintf("opening file %s\n", specFile)
	fileBytes, err := ioutil.ReadFile(testSpecDir + specFile)
	if err != nil {
		return nil, err
	}
	spec, err := model.NewSpec(fileBytes)
	if err != nil {
		return nil, err
	}
	if err = spec.Validate(); err != nil {
		return nil, err
	}
	return spec, err
}

func getTestEvent(eventFile string) []byte {
	fileBytes, _ := ioutil.ReadFile(testEventDir + eventFile)
	return fileBytes
}

func tPrintf(format string, a ...any) {
	if printTestOutput {
		fmt.Printf(format, a...)
	}
}
