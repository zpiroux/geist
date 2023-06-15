package registry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	config := Config{StorageMode: admin.RegStorageNative}
	printTestOutput = false
	tReg = t
	ch := make(entity.NotifyChan, 16)
	logging := false
	ctx := context.Background()
	spec := etltest.SpecSpec()

	// Build the Ingestion Stream for GEIST specs
	stream, err := engine.NewStreamBuilder(etltest.NewStreamEntityFactory()).Build(ctx, spec)
	assert.NoError(t, err)
	regExecutor := engine.NewExecutor(engine.Config{}, stream)
	registry := NewStreamRegistry(config, regExecutor, ch, logging)
	assert.NotNil(t, registry)

	err = registry.Fetch(ctx)
	assert.NoError(t, err)

	specs, err := registry.GetAll(ctx)
	assert.NoError(t, err)

	tPrintf("\n%s\n", "registry.GetAll() returned:")
	for _, spec := range specs {
		tPrintf("Spec with data: %+v\n\n", spec)
	}

	stream, err = engine.NewStreamBuilder(etltest.NewStreamEntityFactory()).Build(ctx, etltest.SpecSpecInMem())
	assert.NoError(t, err)
	regExecutor = engine.NewExecutor(engine.Config{}, stream)
	registry = NewStreamRegistry(config, regExecutor, ch, logging)
	assert.NotNil(t, registry)

	err = registry.Fetch(ctx)
	assert.NoError(t, err)

	specs, err = registry.GetAll(ctx)
	assert.NoError(t, err)
	assert.Zero(t, len(specs))

	tPrintf("\n%s\n", "registry.GetAll() returned:")
	for _, spec := range specs {
		tPrintf("Spec with data: %+v\n\n", spec)
	}
}

const envProd = "prod-xyz"

func TestRunStreamRegistry(t *testing.T) {
	ctx := context.Background()
	spec := etltest.SpecSpec()
	log := false
	notifyChan := make(entity.NotifyChan, 16)
	go handleNotificationEvents(notifyChan)
	time.Sleep(time.Duration(1) * time.Second)

	stream, err := engine.NewStreamBuilder(etltest.NewStreamEntityFactory()).Build(ctx, spec)
	assert.NoError(t, err)
	engineConfig := engine.Config{
		NotifyChan: notifyChan,
		Log:        log,
	}
	regExecutor := engine.NewExecutor(engineConfig, stream)
	registry := NewStreamRegistry(Config{Env: envProd}, regExecutor, notifyChan, log)
	assert.NotNil(t, registry)

	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancel := context.WithCancel(ctx)
	go registry.Run(ctx, &wg)
	time.Sleep(time.Second)

	err = registry.Put(ctx, spec.Id(), spec)
	assert.NoError(t, err)
	storedSpec, err := registry.Get(ctx, spec.Id())
	assert.NoError(t, err)
	assert.Equal(t, spec, storedSpec)

	// Test env handling
	spec, err = entity.NewSpec(specWithOpsPerEnv)
	assert.NoError(t, err)
	assert.Equal(t, spec.OpsPerEnv[envProd].StreamsPerPod, 16)
	assert.Equal(t, spec.Ops.StreamsPerPod, 1)

	err = registry.Put(ctx, spec.Id(), spec)
	assert.NoError(t, err)
	storedSpec, err = registry.Get(ctx, spec.Id())
	assert.NoError(t, err)

	assert.Equal(t, storedSpec.OpsPerEnv[envProd].StreamsPerPod, 16)
	assert.Equal(t, storedSpec.Ops.StreamsPerPod, 16)

	cancel()
	wg.Wait()
}

var (
	specWithOpsPerEnv = []byte(`
{
  "namespace": "geisttest",
  "streamIdSuffix": "opsperenv",
  "version": 1,
  "description": "A spec using different ops per environment",
  "opsPerEnv": {
    "dev": {
      "streamsPerPod": 4
    },
    "staging": {
      "streamsPerPod": 8
    },
    "prod-xyz": {
      "streamsPerPod": 16
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

func handleNotificationEvents(notifyChan entity.NotifyChan) {
	for event := range notifyChan {
		fmt.Printf("%+v\n", event)
	}
}

type RegStreamEntityFactoryMock struct{}

func (r *RegStreamEntityFactoryMock) CreateSinkExtractor(ctx context.Context, spec *entity.Spec) (entity.Extractor, error) {

	m := NewMockSinkExtractor()
	m.loadEventIntoSink(tReg, "../../../test/specs/pubsubsrc-kafkasink-foologs.json")
	m.loadEventIntoSink(tReg, "../../../test/specs/kafkasrc-bigtablesink-user.json")

	return m, nil
}

func (r *RegStreamEntityFactoryMock) CreateExtractor(ctx context.Context, spec *entity.Spec) (entity.Extractor, error) {
	return etltest.NewMockExtractor(spec.Source.Config), nil
}
func (r *RegStreamEntityFactoryMock) CreateTransformer(ctx context.Context, spec *entity.Spec) (igeist.Transformer, error) {
	return etltest.NewMockTransformer(entity.Transform{}), nil
}
func (r *RegStreamEntityFactoryMock) CreateLoader(ctx context.Context, spec *entity.Spec) (entity.Loader, error) {
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
	fileBytes, err := os.ReadFile(eventInFile)
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
