package xkafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

type Producer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Events() chan kafka.Event
	Flush(timeoutMs int) int
	Close()
}

type ProducerFactory interface {
	NewProducer(conf *kafka.ConfigMap) (Producer, error)
	NewAdminClientFromProducer(p Producer) (AdminClient, error)
}

type DefaultProducerFactory struct{}

func (d DefaultProducerFactory) NewProducer(conf *kafka.ConfigMap) (Producer, error) {
	return kafka.NewProducer(conf)
}

func (d DefaultProducerFactory) NewAdminClientFromProducer(p Producer) (AdminClient, error) {
	return kafka.NewAdminClientFromProducer(p.(*kafka.Producer))
}
