package registry

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/teltech/logger"
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/entity/transform"
	"github.com/zpiroux/geist/internal/pkg/admin"
	"github.com/zpiroux/geist/internal/pkg/engine"
	"github.com/zpiroux/geist/internal/pkg/etltest"
	"github.com/zpiroux/geist/internal/pkg/igeist"
)

var (
	printTestOutput bool
	tReg            *testing.T
)

func TestStreamRegistryFetch(t *testing.T) {

	log = log.WithLevel(logger.DEBUG)

	config := Config{StorageMode: admin.RegStorageNative}
	printTestOutput = false
	tReg = t
	ctx := context.Background()
	spec := etltest.SpecSpec()

	// Build the Ingestion Stream for GEIST specs
	stream, err := engine.NewStreamBuilder(etltest.NewStreamEntityFactory()).Build(ctx, spec)
	assert.NoError(t, err)
	regExecutor := engine.NewExecutor(engine.Config{}, stream)
	registry := NewStreamRegistry(config, regExecutor)
	assert.NotNil(t, registry)

	err = registry.Fetch(ctx)
	assert.NoError(t, err)

	specs, err := registry.GetAll(ctx)
	assert.NoError(t, err)

	tPrintf("\n%s\n", "registry.GetAll() returned:")
	for _, spec := range specs {
		tPrintf("Spec with data: %+v\n\n", spec.(*entity.Spec))
	}

	// TODO: Add test for in-mem
	stream, err = engine.NewStreamBuilder(etltest.NewStreamEntityFactory()).Build(ctx, etltest.SpecSpecInMem())
	assert.NoError(t, err)
	regExecutor = engine.NewExecutor(engine.Config{}, stream)
	registry = NewStreamRegistry(config, regExecutor)
	assert.NotNil(t, registry)

	err = registry.Fetch(ctx)
	assert.NoError(t, err)

	specs, err = registry.GetAll(ctx)
	assert.NoError(t, err)
	assert.Zero(t, len(specs))

	tPrintf("\n%s\n", "registry.GetAll() returned:")
	for _, spec := range specs {
		tPrintf("Spec with data: %+v\n\n", spec.(*entity.Spec))
	}
}

type RegStreamEntityFactoryMock struct{}

func (r *RegStreamEntityFactoryMock) CreateSinkExtractor(ctx context.Context, spec igeist.Spec) (entity.Extractor, error) {

	m := NewMockSinkExtractor()
	m.loadEventIntoSink(tReg, "../../../test/specs/pubsubsrc-kafkasink-foologs.json")
	m.loadEventIntoSink(tReg, "../../../test/specs/kafkasrc-bigtablesink-user.json")

	return m, nil
}

func (r *RegStreamEntityFactoryMock) CreateExtractor(ctx context.Context, spec igeist.Spec) (entity.Extractor, error) {
	return etltest.NewMockExtractor(spec.(*entity.Spec).Source.Config), nil
}
func (r *RegStreamEntityFactoryMock) CreateTransformer(ctx context.Context, spec igeist.Spec) (igeist.Transformer, error) {
	return etltest.NewMockTransformer(entity.Transform{}), nil
}
func (r *RegStreamEntityFactoryMock) CreateLoader(ctx context.Context, spec igeist.Spec) (entity.Loader, error) {
	return etltest.NewMockLoader(), nil
}

type MockSinkExtractor struct {
	Transformer *transform.Transformer
	specRepo    []*entity.Transformed
}

func NewMockSinkExtractor() *MockSinkExtractor {

	var m MockSinkExtractor
	m.Transformer = transform.NewTransformer(etltest.SpecSpec())
	return &m
}

func (m *MockSinkExtractor) StreamExtract(
	ctx context.Context,
	reportEvent entity.ProcessEventFunc,
	err *error,
	retryable *bool) {

	*err = errors.New("not applicable")
}

func (m *MockSinkExtractor) Extract(ctx context.Context, query entity.ExtractorQuery, result any) (error, bool) {
	return nil, false
}

func (m *MockSinkExtractor) ExtractFromSink(ctx context.Context, query entity.ExtractorQuery, result *[]*entity.Transformed) (error, bool) {

	// For now, assume it's an 'All' query
	*result = append(*result, m.specRepo...)

	return nil, false
}

// Channel based extractor functionality not provided from this Mock Extractor
func (m *MockSinkExtractor) SendToSource(ctx context.Context, eventData any) (string, error) {
	return "", nil
}

func (m *MockSinkExtractor) loadEventIntoSink(t *testing.T, eventInFile string) {
	var retryable bool
	fileBytes, err := ioutil.ReadFile(eventInFile)
	assert.NoError(t, err)
	output, err := m.Transformer.Transform(context.Background(), fileBytes, &retryable)
	assert.NoError(t, err)
	require.NotNil(t, output)
	tPrintf("Transformation output, len: %d\n", len(output))

	m.specRepo = append(m.specRepo, output[0])
	assert.NoError(t, err)
}

func tPrintf(format string, a ...any) {
	if printTestOutput {
		fmt.Printf(format, a...)
	}
}
