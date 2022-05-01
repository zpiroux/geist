package xkafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

type Consumer interface {
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error
	Poll(timeoutMs int) (event kafka.Event)
	StoreOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error)
	Commit() ([]kafka.TopicPartition, error)
	CommitMessage(m *kafka.Message) ([]kafka.TopicPartition, error)
	Close() error
}

type ConsumerFactory interface {
	NewConsumer(conf *kafka.ConfigMap) (Consumer, error)
	NewAdminClientFromConsumer(c Consumer) (AdminClient, error)
}
type DefaultConsumerFactory struct{}

func (d DefaultConsumerFactory) NewConsumer(conf *kafka.ConfigMap) (Consumer, error) {
	return kafka.NewConsumer(conf)
}

func (d DefaultConsumerFactory) NewAdminClientFromConsumer(c Consumer) (AdminClient, error) {
	ac, err := kafka.NewAdminClientFromConsumer(c.(*kafka.Consumer))
	if err != nil {
		return nil, err
	}
	return DefaultAdminClient{ac: ac}, nil
}
