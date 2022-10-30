package channel

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/internal/pkg/admin"
)

const (
	testDirPath  = "../../../../test/"
	testSpecDir  = testDirPath + "specs/"
	testEventDir = testDirPath + "events/"
)

var (
	SpecRegSpec               = admin.SpecRegistrationSpec
	SpecRegChangeNotification = admin.AdminEventSpec
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

func TestExtractorStreamExtract(t *testing.T) {

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

func reportEvent(ctx context.Context, events []entity.Event) entity.EventProcessingResult {

	tPrintf("In reportEvent in Mock Executor, events: %+v\n", events)

	return entity.EventProcessingResult{ResourceId: "no-resource-id"}
}

func newTestExtractor(t *testing.T, specBytes []byte) *extractor {

	spec, err := entity.NewSpec(specBytes)
	assert.NoError(t, err)

	extractor := newExtractor(entity.Config{Spec: spec, ID: "instanceId"})
	assert.NotNil(t, extractor)

	return extractor
}

func newTestExtractorFromFile(t *testing.T, specId string) *extractor {

	spec, err := getTestSpec(specId)
	assert.NoError(t, err)
	assert.NotNil(t, spec)

	extractor := newExtractor(entity.Config{Spec: spec, ID: "instanceId"})
	assert.NotNil(t, extractor)

	return extractor
}

func getTestSpec(specFile string) (*entity.Spec, error) {
	tPrintf("opening file %s\n", specFile)
	fileBytes, err := os.ReadFile(testSpecDir + specFile)
	if err != nil {
		return nil, err
	}
	spec, err := entity.NewSpec(fileBytes)
	if err != nil {
		return nil, err
	}
	if err = spec.Validate(); err != nil {
		return nil, err
	}
	return spec, err
}

func getTestEvent(eventFile string) []byte {
	fileBytes, _ := os.ReadFile(testEventDir + eventFile)
	return fileBytes
}

func tPrintf(format string, a ...any) {
	if printTestOutput {
		fmt.Printf(format, a...)
	}
}
