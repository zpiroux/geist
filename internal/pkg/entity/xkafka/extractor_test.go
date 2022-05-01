package xkafka

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/internal/pkg/model"
)

var (
	eventsToConsume = 3
	eventCount      = 0
	allHoueModes    = []string{model.HoueDefault, model.HoueDiscard, model.HoueDlq, model.HoueFail}
)

func TestMicroBatchTimeout(t *testing.T) {

	mbTimeoutMs := 5000
	now := time.Now()
	timedOut := microBatchTimedOut(now, mbTimeoutMs)
	assert.False(t, timedOut)

	start := time.Now()
	time.Sleep(7 * time.Second)
	timedOut = microBatchTimedOut(start, mbTimeoutMs)
	assert.True(t, timedOut)
}

func TestExtractor(t *testing.T) {

	var (
		err       error
		retryable bool
	)
	spec := GetMockSpec()
	spec.StreamIdSuffix = "happy-path"
	spec.Ops.HandlingOfUnretryableEvents = model.HoueDefault
	extractor := createExtractor(spec, t)
	ctx, cancel := context.WithCancel(context.Background())

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
	assert.Equal(t, eventsToConsume, eventCount)
	assert.Equal(t, eventCount, int(extractor.eventCount+1))
	assert.Equal(t, "value1", (*(extractor.consumer.(*MockConsumer)).conf)["prop1"])
	assert.Equal(t, "value2", (*(extractor.consumer.(*MockConsumer)).conf)["prop2"])
	assert.Equal(t, "value3", (*(extractor.consumer.(*MockConsumer)).conf)["prop3"])
	cancel()
}

func TestExtractor_RetryableFailure(t *testing.T) {

	var (
		err       error
		retryable bool
	)

	for _, houeMode := range allHoueModes {
		eventCount = 0
		spec := GetMockSpec()
		spec.StreamIdSuffix = "retryable-failure"
		spec.Ops.HandlingOfUnretryableEvents = houeMode
		extractor := createExtractor(spec, t)

		extractor.StreamExtract(
			context.Background(),
			reportEvent_RetryableFailure,
			&err,
			&retryable)

		log.Debugf("stream extract returned err: %v", err)
		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), ErrRetriesExhausted))
		assert.True(t, retryable)
	}
}

func TestExtractor_UnretryableFailure(t *testing.T) {

	var (
		err       error
		retryable bool
	)

	for _, houeMode := range allHoueModes {
		eventCount = 0
		spec := GetMockSpec()
		spec.StreamIdSuffix = "unretryable-failure"
		spec.Ops.HandlingOfUnretryableEvents = houeMode
		extractor := createExtractor(spec, t)
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			extractor.StreamExtract(
				ctx,
				reportEvent_UnretryableFailure,
				&err,
				&retryable)
			wg.Done()
		}()

		time.Sleep(time.Duration(3) * time.Second)
		cancel()
		wg.Wait()

		if houeMode == model.HoueFail {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
		assert.False(t, retryable)
	}
}

var (
	nbPublishToFail   int
	nbPublishRequests int
)

func TestMoveToDLQ(t *testing.T) {

	var (
		cf  MockConsumerFactory
		ctx = context.Background()
		err error
	)

	spec := GetMockSpec()
	spec.Ops.HandlingOfUnretryableEvents = model.HoueDlq
	config := NewExtractorConfig(spec, []string{"coolTopic"}, &sync.Mutex{})

	config.SetPollTimout(2000)
	extractor, err := NewExtractor(config, "mockInstanceId")
	assert.NoError(t, err)
	extractor.SetConsumerFactory(cf)

	msg := &kafka.Message{
		Value: []byte("niceValue"),
	}

	nbPublishToFail = 0
	nbPublishRequests = 0
	extractor.SetProducerFactory(MockDlqProducerFactory{})
	err = extractor.initStreamExtract(ctx)
	assert.NoError(t, err)
	extractor.moveEventToDLQ(ctx, msg)
	assert.Equal(t, nbPublishToFail+1, nbPublishRequests)

	nbPublishToFail = 1
	nbPublishRequests = 0
	extractor.dlqProducer.(*MockDlqProducer).nbFailedPublishReported = 0
	extractor.moveEventToDLQ(ctx, msg)
	assert.Equal(t, nbPublishToFail+1, nbPublishRequests)

	nbPublishToFail = 3
	nbPublishRequests = 0
	extractor.dlqProducer.(*MockDlqProducer).nbFailedPublishReported = 0
	extractor.moveEventToDLQ(ctx, msg)
	assert.Equal(t, nbPublishToFail+1, nbPublishRequests)
}

func createExtractor(spec *model.Spec, t *testing.T) *Extractor {
	config := NewExtractorConfig(spec, []string{"coolTopic"}, &sync.Mutex{})

	config.SetPollTimout(2000)
	config.SetProps(ConfigMap{
		"prop1": "value1",
	})
	config.SetKafkaProperty("prop2", "value2")
	config.SetProps(ConfigMap{
		"prop3": "value3",
	})
	extractor, err := NewExtractor(config, "mockInstanceId")
	assert.NoError(t, err)
	extractor.SetConsumerFactory(MockConsumerFactory{})
	extractor.SetProducerFactory(MockDlqProducerFactory{})
	return extractor
}

type MockConsumer struct {
	conf *kafka.ConfigMap
}

func (m *MockConsumer) SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error {
	return nil
}

func (m *MockConsumer) Poll(timeoutMs int) kafka.Event {
	time.Sleep(time.Duration(timeoutMs) * time.Millisecond)
	eventCount += 1
	return &kafka.Message{
		Value:     []byte("foo " + strconv.Itoa(eventCount)),
		Timestamp: time.Now().UTC(),
	}
}
func (m *MockConsumer) StoreOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	var tp []kafka.TopicPartition
	return tp, nil
}

func (m *MockConsumer) CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	var tp []kafka.TopicPartition
	return tp, nil
}

func (m *MockConsumer) Commit() ([]kafka.TopicPartition, error) {
	var tp []kafka.TopicPartition
	return tp, nil
}

func (m *MockConsumer) Close() error {
	return nil
}

type MockConsumerFactory struct{}

func (mcf MockConsumerFactory) NewConsumer(conf *kafka.ConfigMap) (Consumer, error) {
	return &MockConsumer{conf: conf}, nil
}

func (mcf MockConsumerFactory) NewAdminClientFromConsumer(c Consumer) (AdminClient, error) {
	return &MockAdminClient{}, nil
}

type MockDlqProducerFactory struct{}

func (mpf MockDlqProducerFactory) NewProducer(conf *kafka.ConfigMap) (Producer, error) {
	return NewMockDlqProducer(), nil
}

func (mpf MockDlqProducerFactory) NewAdminClientFromProducer(p Producer) (AdminClient, error) {
	return &MockAdminClient{}, nil
}

func reportEvent(ctx context.Context, events []model.Event) model.EventProcessingResult {
	if eventCount >= eventsToConsume {
		return model.EventProcessingResult{
			Status: model.ExecutorStatusShutdown,
			Error:  fmt.Errorf("event processing of event %v aborted due to shutdown signal", events),
		}
	}
	return model.EventProcessingResult{
		Status:    model.ExecutorStatusSuccessful,
		Retryable: true,
	}
}

const ErrRetriesExhausted = "executor reached max retry limit"

func reportEvent_RetryableFailure(ctx context.Context, events []model.Event) model.EventProcessingResult {
	return model.EventProcessingResult{
		Status:    model.ExecutorStatusRetriesExhausted,
		Error:     fmt.Errorf(ErrRetriesExhausted+", for event: %+v", events),
		Retryable: true,
	}
}

const ErrUnretryableError = "executor encountered an unretryable error"

func reportEvent_UnretryableFailure(ctx context.Context, events []model.Event) model.EventProcessingResult {
	return model.EventProcessingResult{
		Status:    model.ExecutorStatusError,
		Error:     fmt.Errorf(ErrUnretryableError+", for event: %+v", events),
		Retryable: false,
	}
}

type MockAdminClient struct{}

func (m MockAdminClient) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error) {
	return &kafka.Metadata{}, nil
}

func (m MockAdminClient) CreateTopics(ctx context.Context, topics []kafka.TopicSpecification, options ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error) {
	var result kafka.TopicResult
	result.Topic = topics[0].Topic
	return []kafka.TopicResult{result}, nil
}

func NewMockDlqProducer() Producer {
	p := &MockDlqProducer{}
	p.events = make(chan kafka.Event, 10)
	return p
}

type MockDlqProducer struct {
	events                  chan kafka.Event
	nbFailedPublishReported int
}

func (p *MockDlqProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	nbPublishRequests++

	dChan := p.events

	if deliveryChan != nil {
		dChan = deliveryChan
	}

	if p.nbFailedPublishReported < nbPublishToFail {
		p.nbFailedPublishReported++

		switch p.nbFailedPublishReported {
		case 1:
			return fmt.Errorf("publish enqueue failed, nb failures: %d", p.nbFailedPublishReported)
		case 2:
			msg.TopicPartition.Error = fmt.Errorf("publish kafka op failed, nb failures: %d", p.nbFailedPublishReported)
			dChan <- msg
			return nil
		case 3:
			errCodeCoordinatorNotAvailable := 15 // this value is no longer exported properly from go lib
			dChan <- kafka.NewError(kafka.ErrorCode(errCodeCoordinatorNotAvailable), "bad stuff", false)
			return nil
		default:
			return fmt.Errorf("random bad thing happened, nb failures: %d", p.nbFailedPublishReported)
		}
	}
	msg.TopicPartition.Error = nil
	dChan <- msg
	return nil
}

func (p MockDlqProducer) Events() chan kafka.Event {
	return p.events
}

func (p MockDlqProducer) Flush(timeoutMs int) int {
	return 0
}

func (p MockDlqProducer) Close() {}

func GetMockSpec() *model.Spec {

	spec := model.NewEmptySpec()
	spec.Namespace = "extractor"
	spec.StreamIdSuffix = "mockspec"
	spec.Version = 1
	spec.Description = "..."
	return spec
}
