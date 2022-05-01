package xpubsub

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/internal/pkg/etltest"
	"github.com/zpiroux/geist/internal/pkg/model"
)

const (
	testDirPath  = "../../../../test/"
	testSpecDir  = testDirPath + "specs/"
	testEventDir = testDirPath + "events/"
)

var printTestOutput bool

func TestNewExtractor(t *testing.T) {

	spec, err := model.NewSpec(model.AdminEventSpec)
	assert.NoError(t, err)

	printTestOutput = true
	id := "mockId"
	ec := &ExtractorConfig{
		client: &MockClient{},
		spec:   spec,
	}

	client := MockClient{}
	ctx := context.Background()

	tPrintf("Starting TestNewExtractor\n")

	// Check handling of incorrect input
	_, err = NewExtractor(ctx, ec, id)
	assert.Error(t, err)

	ec.client = &client
	_, err = NewExtractor(ctx, ec, id)
	assert.Error(t, err)

	// Valid input
	extractor := newTestExtractor(t, etltest.SpecRegSpecPubsub)
	assert.NotNil(t, extractor)

	extractor = newTestExtractor(t, etltest.SpecPubsubSrcKafkaSinkFoologs)
	assert.NotNil(t, extractor)
}

// This test currently test extraction of a single event (sent by MockSubscription.Receive()
func TestExtractor_StreamExtract(t *testing.T) {

	var (
		err       error
		retryable bool
	)
	ctx := context.Background()

	tPrintf("Starting TestExtractor_StreamExtract\n")

	extractor := newTestExtractor(t, etltest.SpecRegSpecPubsub)
	assert.NotNil(t, extractor)

	extractor.SetSub(&MockSubscription{})
	extractor.SetMsgAckNackFunc(ack, nack)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		extractor.StreamExtract(
			ctx,
			reportEvent,
			&err,
			&retryable)
		wg.Done()
	}()
	wg.Wait()
	assert.NoError(t, err)

	extractor = newTestExtractorWithAdminStream(t)
	assert.NotNil(t, extractor)

	extractor.SetSub(&MockSubscription{})
	extractor.SetMsgAckNackFunc(ack, nack)

	wg.Add(1)
	go func() {
		extractor.StreamExtract(
			ctx,
			reportEvent,
			&err,
			&retryable)
		wg.Done()
	}()
	wg.Wait()
	assert.NoError(t, err)
}

func reportEvent(ctx context.Context, events []model.Event) model.EventProcessingResult {
	return model.EventProcessingResult{Status: model.ExecutorStatusSuccessful}
}

func TestExtractor_SendToSource(t *testing.T) {

	var err error

	client := &MockClient{}
	ctx := context.Background()

	spec, err := model.NewSpec(model.AdminEventSpec)
	assert.NoError(t, err)

	ec := NewExtractorConfig(client, spec, []string{"coolTopic"}, ReceiveSettings{})
	extractor, err := NewExtractor(ctx, ec, "mockId")
	assert.NoError(t, err)
	assert.NotNil(t, extractor)

	extractor.SetTopic(&MockTopic{})
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_, err := extractor.SendToSource(ctx, "Hi! I'm an event as string")
		assert.EqualError(t, err, "context canceled")
		_, err = extractor.SendToSource(ctx, []byte("Hi! I'm an event as bytes"))
		assert.EqualError(t, err, "context canceled")
		_, err = extractor.SendToSource(ctx, 123456789)
		assert.EqualError(t, err, "invalid type for eventData (int), only string and []byte allowed")
		wg.Done()
	}()
	cancel()
	wg.Wait()
}

func newTestExtractor(t *testing.T, specId string) *Extractor {

	client := &MockClient{}
	ctx := context.Background()

	registry := etltest.NewStreamRegistry(testDirPath)
	err := registry.Fetch(ctx)
	assert.NoError(t, err)

	rawSpec, err := registry.Get(ctx, specId)
	assert.NoError(t, err)
	assert.NotNil(t, rawSpec)

	spec := rawSpec.(*model.Spec)

	ec := NewExtractorConfig(client, spec, []string{"coolTopic"}, ReceiveSettings{})
	extractor, err := NewExtractor(ctx, ec, "mockId")
	assert.NoError(t, err)
	assert.NotNil(t, extractor)

	return extractor
}

func newTestExtractorWithAdminStream(t *testing.T) *Extractor {

	client := &MockClient{}
	ctx := context.Background()
	spec, err := model.NewSpec(model.AdminEventSpec)
	assert.NoError(t, err)

	ec := NewExtractorConfig(client, spec, []string{"coolTopic"}, ReceiveSettings{})
	extractor, err := NewExtractor(ctx, ec, "mockId")
	assert.NoError(t, err)
	assert.NotNil(t, extractor)

	return extractor
}

type MockClient struct{}

func (m *MockClient) Topic(id string) *pubsub.Topic {
	return &pubsub.Topic{}
}

func (m *MockClient) CreateSubscription(ctx context.Context, id string, cfg pubsub.SubscriptionConfig) (*pubsub.Subscription, error) {
	return &pubsub.Subscription{}, nil
}

func (m *MockClient) Subscription(id string) *pubsub.Subscription {
	return &pubsub.Subscription{}
}

type MockSubscription struct {
	name string
}

// The real Receive() runs until canceled, but this mock one currently only sends one event
// and then exits without error.
// TODO: Add more scenarios
func (s *MockSubscription) Receive(ctx context.Context, f func(context.Context, *pubsub.Message)) error {

	tPrintf("In Receive in MockSubscription\n")

	msg := pubsub.Message{
		Data:        []byte("foo"),
		ID:          "mockMsgId",
		PublishTime: time.Now(),
	}

	f(ctx, &msg)

	return nil
}

func (s *MockSubscription) Delete(ctx context.Context) error {
	return nil
}

func (s *MockSubscription) String() string {
	return s.name
}

type MockSubConfigurator struct{}

func (m *MockSubConfigurator) Update(sub Subscription, rs pubsub.ReceiveSettings) {

}

type MockTopic struct {
}

func (t *MockTopic) Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult {

	return &pubsub.PublishResult{}

}

func ack(m *pubsub.Message) {
	tPrintf("Ack called with msg.Data: %s, full msg: %+v\n", string(m.Data), m)
}

func nack(m *pubsub.Message) {
	tPrintf("Nack called with msg.Data: %s, full msg: %+v\n", string(m.Data), m)
}

func tPrintf(format string, a ...any) {
	if printTestOutput {
		fmt.Printf(format, a...)
	}
}
