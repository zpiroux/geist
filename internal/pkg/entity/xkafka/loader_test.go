package xkafka

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/internal/pkg/entity/transform"
	"github.com/zpiroux/geist/internal/pkg/model"
)

func TestLoader_StreamLoad(t *testing.T) {

	var (
		retryable        bool
		eventFromSourceX = []byte(`{ "veryInterestingDataField": "omg" }`)
	)

	spec, err := model.NewSpec(genericSourceToKafkaSinkSpec)
	assert.NoError(t, err)
	assert.NotNil(t, spec)
	loader := createLoader(spec, true, t)
	transformer := transform.NewTransformer(spec)

	transformed, err := transformer.Transform(context.Background(), eventFromSourceX, &retryable)
	assert.NoError(t, err)
	assert.NotEqual(t, len(transformed), 0)
	fmt.Printf("event transformed into: %v\n", transformed)

	_, err, retryable = loader.StreamLoad(context.Background(), transformed)
	assert.NoError(t, err)
}

func createLoader(spec *model.Spec, synchronous bool, t *testing.T) *Loader {
	topicSpec := &model.TopicSpecification{
		Name:              "coolTopic",
		NumPartitions:     3,
		ReplicationFactor: 3,
	}
	config := NewLoaderConfig(spec, topicSpec, &sync.Mutex{}, synchronous)
	loader, err := NewLoader(context.Background(), config, "mockInstanceId", MockProducerFactory{})
	assert.NoError(t, err)
	return loader
}

type MockProducerFactory struct{}

func (mpf MockProducerFactory) NewProducer(conf *kafka.ConfigMap) (Producer, error) {
	return NewMockProducer(), nil
}

func (mpf MockProducerFactory) NewAdminClientFromProducer(p Producer) (AdminClient, error) {
	return &MockAdminClient{}, nil
}

func NewMockProducer() Producer {
	p := &MockProducer{}
	p.events = make(chan kafka.Event, 10)
	return p
}

type MockProducer struct {
	events chan kafka.Event
}

func (p *MockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	nbPublishRequests++
	dChan := p.events
	if deliveryChan != nil {
		dChan = deliveryChan
	}
	msg.TopicPartition.Error = nil
	dChan <- msg
	return nil
}

func (p MockProducer) Events() chan kafka.Event {
	return p.events
}

func (p MockProducer) Flush(timeoutMs int) int {
	return 0
}

func (p MockProducer) Close() {}

var genericSourceToKafkaSinkSpec = []byte(`
{
   "namespace": "geist",
   "streamIdSuffix": "xtokafkaproxy",
   "description": "Generic spec for any source forwarding raw event into a Kafka topic",
   "version": 1,
   "ops": {
      "logEventData": true
   },
   "source": {
      "type": "kafka"
   },
   "transform": {
      "extractFields": [
         {
            "fields": [
               {
                  "id": "payload"
               }
            ]
         }
      ]
   },
   "sink": {
      "type": "kafka",
      "config": {
         "topic": [
            {
               "env": "all",
               "topicSpec": {
                  "name": "events_from_source_x",
                  "numPartitions": 6,
                  "replicationFactor": 3
               }
            }
         ],
         "properties": [
            {
               "key": "client.id",
               "value": "geist_xtokafkaproxy"
            }
         ],
         "message": {
            "payloadFromId": "payload"
         }
      }
   }
}`)
