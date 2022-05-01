package xkafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type AdminClient interface {
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error)
	CreateTopics(ctx context.Context, topics []kafka.TopicSpecification, options ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error)
}

func topicExists(topicToFind string, existingTopics map[string]kafka.TopicMetadata) bool {
	for _, topic := range existingTopics {
		if topic.Topic == topicToFind {
			return true
		}
	}
	return false
}

type DefaultAdminClient struct {
	ac *kafka.AdminClient
}

func (d DefaultAdminClient) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error) {
	return d.ac.GetMetadata(topic, allTopics, timeoutMs)
}

func (d DefaultAdminClient) CreateTopics(ctx context.Context, topics []kafka.TopicSpecification, options ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error) {
	return d.ac.CreateTopics(ctx, topics, options...)
}
