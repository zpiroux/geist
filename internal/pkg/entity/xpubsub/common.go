package xpubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
)

type PubsubClient interface {
	Topic(id string) *pubsub.Topic
	CreateSubscription(ctx context.Context, id string, cfg pubsub.SubscriptionConfig) (*pubsub.Subscription, error)
	Subscription(id string) *pubsub.Subscription
}

type Topic interface {
	Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult
}
