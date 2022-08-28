package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/entity/transform"
	"github.com/zpiroux/geist/internal/pkg/admin"
	"github.com/zpiroux/geist/internal/pkg/etltest"
	"github.com/zpiroux/geist/internal/pkg/igeist"
)

const testDirPath = "../../../test/"

var printTestOutput = true

// Supervisor testing also tests StreamEntityFactory, StreamBuilder and Executor

type participants struct {
	factory    igeist.StreamEntityFactory
	builder    igeist.StreamBuilder
	registry   igeist.StreamRegistry
	supervisor *Supervisor
}

func TestSupervisor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := gatherParticipants(t, ctx)
	ready := &sync.WaitGroup{}
	ready.Add(1)

	go doStuff(cancel)

	err := p.supervisor.Run(ctx, ready)
	assert.NoError(t, err)
}

func TestAdminEventHandlerStreamLoad(t *testing.T) {

	tPrintf("\nTestAdminEventHandler_StreamLoad\n")
	ctx, cancel := context.WithCancel(context.Background())

	p := gatherParticipants(t, ctx)
	adminEventHandler := p.supervisor.AdminEventHandler()
	ready := &sync.WaitGroup{}
	ready.Add(1)

	go func() {
		err := p.supervisor.Run(ctx, ready)
		assert.NoError(t, err)
	}()

	time.Sleep(2 * time.Second)
	transformed := createTransformationOutput(t)

	id, err, _ := adminEventHandler.StreamLoad(ctx, transformed)
	assert.NoError(t, err)
	tPrintf("StreamLoad returned id: %s", id)

	time.Sleep(2 * time.Second)
	cancel()
}

func TestUpgradingOneExecutor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	p := gatherParticipants(t, ctx)
	adminEventHandler := p.supervisor.AdminEventHandler()
	ready := &sync.WaitGroup{}
	ready.Add(1)

	go func() {
		err := p.supervisor.Run(ctx, ready)
		assert.NoError(t, err)
	}()

	time.Sleep(2 * time.Second)
	transformed := createTransformationOutput(t)
	_, err, _ := adminEventHandler.StreamLoad(ctx, transformed)
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)
	transformed = createTransformationOutput(t)
	_, err, _ = adminEventHandler.StreamLoad(ctx, transformed)
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)
	cancel()
	time.Sleep(2 * time.Second)
}

func createTransformationOutput(t *testing.T) []*entity.Transformed {
	retryable := false
	spec, err := entity.NewSpec(admin.AdminEventSpec)
	assert.NoError(t, err)
	assert.NotNil(t, spec)

	eventBytes, err := createRegistryModifiedEvent(etltest.SpecApiSrcBigtableSinkMinimal)
	assert.NoError(t, err)

	transformer := transform.NewTransformer(spec)
	output, err := transformer.Transform(context.Background(), eventBytes, &retryable)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	tPrintf("Transformation output: %+v\n", output)
	return output
}

func createRegistryModifiedEvent(streamId string) ([]byte, error) {

	event := admin.NewAdminEvent(
		admin.EventStreamRegistryModified,
		admin.OperationStreamRegistration,
		streamId)

	return json.Marshal(event)
}

func gatherParticipants(t *testing.T, ctx context.Context) participants {
	var (
		p      participants
		config Config
	)
	p.registry = etltest.NewStreamRegistry(testDirPath)
	err := p.registry.Fetch(ctx)
	assert.NoError(t, err)

	p.factory = etltest.NewStreamEntityFactory()
	p.builder = NewStreamBuilder(p.factory)
	p.supervisor, err = NewSupervisor(ctx, config, p.builder, p.registry)
	assert.NoError(t, err)
	assert.NotNil(t, p.supervisor)

	p.createAdminEventStream(t, ctx)

	err = p.supervisor.Init(ctx)
	assert.NoError(t, err)
	return p
}

func doStuff(cancel context.CancelFunc) {
	time.Sleep(2 * time.Second)
	cancel()
}

func (p participants) createAdminEventStream(t *testing.T, ctx context.Context) {
	p.factory.SetAdminLoader(p.supervisor.AdminEventHandler())
	spec, err := entity.NewSpec(admin.AdminEventSpec)
	assert.NoError(t, err)

	stream, err := p.builder.Build(ctx, spec)
	assert.NoError(t, err)

	adminEventStreamExecutor := NewExecutor(Config{}, stream)
	p.supervisor.RegisterExecutor(ctx, adminEventStreamExecutor)
	p.registry.SetAdminStream(stream)
}

func tPrintf(format string, a ...any) {
	if printTestOutput {
		fmt.Printf(format, a...)
	}
}
