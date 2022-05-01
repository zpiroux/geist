package xkafka

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/zpiroux/geist/internal/pkg/model"
)

const (
	metadataRequestTimeoutMs    = 5000
	dlqTopicPrefix              = "geist.dlq."
	dlqMaxPublishBackoffTimeSec = 30
	dlqTopicNumPartitions       = 6
	dlqTopicReplicationFactor   = 3
)

func (e *Extractor) createDlqTopic(ctx context.Context, dlqTopic string) error {

	if e.config.spec.Ops.HandlingOfUnretryableEvents != model.HoueDlq {
		return nil
	}

	e.config.topicCreationMutex.Lock()
	defer e.config.topicCreationMutex.Unlock()

	dlqTopicMetadata, err := e.ac.GetMetadata(nil, true, metadataRequestTimeoutMs)
	if err != nil {
		return fmt.Errorf(e.lgprfx()+"could not get metadata from Kafka cluster, err: %v", err)
	}

	if !topicExists(dlqTopic, dlqTopicMetadata.Topics) {

		log.Infof(e.lgprfx()+"DLQ topic doesn't exist for stream %s, metadata: %+v, err: %v", e.config.spec.Id(), dlqTopicMetadata, err)
		dlq := kafka.TopicSpecification{
			Topic:             dlqTopic,
			NumPartitions:     dlqTopicNumPartitions,
			ReplicationFactor: dlqTopicReplicationFactor,
		}

		res, err := e.ac.CreateTopics(ctx, []kafka.TopicSpecification{dlq})

		if err != nil {
			return fmt.Errorf(e.lgprfx()+"could not create DLQ topic %+v, err: %v", dlq, err)
		}
		log.Infof(e.lgprfx()+"DLQ topic created: %+v", res)
	} else {
		log.Infof(e.lgprfx()+"DLQ topic for this stream already exists with name: %s", dlqTopic)
	}

	return nil
}

func (e *Extractor) createDlqProducer(pf ProducerFactory) error {

	if e.config.spec.Ops.HandlingOfUnretryableEvents != model.HoueDlq {
		return nil
	}

	var err error
	kconfig := make(kafka.ConfigMap)

	// Add producer specific non-optional props
	kconfig["enable.idempotence"] = true
	kconfig["compression.type"] = "lz4"
	kconfig["acks"] = "all"
	kconfig["max.in.flight.requests.per.connection"] = 5

	for k, v := range e.config.configMap {
		kconfig[k] = v
	}

	e.dlqProducer, err = e.pf.NewProducer(&kconfig)

	if err != nil {
		return fmt.Errorf(e.lgprfx()+"Failed to create DLQ producer: %s", err.Error())
	}

	log.Infof(e.lgprfx()+"Created DLQ Producer %+v, with config: %+v", e.dlqProducer, kconfig)
	return nil
}

func (e *Extractor) dlqTopicName() string {
	return dlqTopicPrefix + e.config.spec.Id()
}

func (e *Extractor) moveEventsToDLQ(ctx context.Context, msgs []*kafka.Message) action {

	var a action
	for _, m := range msgs {
		a = e.moveEventToDLQ(ctx, m)
		if a == actionShutdown {
			break
		}
	}
	return a
}

func (e *Extractor) moveEventToDLQ(ctx context.Context, m *kafka.Message) action {

	// Infinite loop here to ensure we never loose a message. For long disruptions this consumer's partitions
	// will be reassigned to another consumer after the session times out, which in worst case might lead to
	// duplicates, but no loss.
	var err error
	backoffDuration := 1
	dlqTopic := e.dlqTopicName()

	mCopy := *m
	mCopy.TopicPartition.Topic = &dlqTopic
	mCopy.TopicPartition.Partition = kafka.PartitionAny
	for i := 0; ; i++ {

		if ctx.Err() == context.Canceled {
			log.Warnf(e.lgprfx() + "context canceled received in DLQ producer, shutting down")
			return actionShutdown
		}

		log.Debugf(e.lgprfx()+"sending event to DLQ with producer: %+v", e.dlqProducer)

		err = e.dlqProducer.Produce(&mCopy, nil)

		log.Debugf(e.lgprfx()+"Produce() returned err: %v", err)

		if err == nil {
			event := <-e.dlqProducer.Events()
			switch dlqMsg := event.(type) {
			case *kafka.Message:
				if dlqMsg.TopicPartition.Error != nil {
					err = fmt.Errorf("DLQ publish failed with err: %v", dlqMsg.TopicPartition.Error)
				} else {
					log.Infof(e.lgprfx()+"event published to DLQ %s [%d] at offset %v, value: %s",
						*dlqMsg.TopicPartition.Topic, dlqMsg.TopicPartition.Partition,
						dlqMsg.TopicPartition.Offset, string(dlqMsg.Value))

					// TODO: When migrated to new common producer, move this out to Extractor
					if serr := e.storeOffsets([]*kafka.Message{m}); serr != nil {
						// No need to handle this error, just log it
						log.Errorf(e.lgprfx()+"error storing offsets after dlq, event: %+v, err: %v", m, serr)
					}
					return actionContinue
				}
			case kafka.Error:
				err = fmt.Errorf("kafka error in dlq producer, code: %v, event: %v", dlqMsg.Code(), dlqMsg)
				// In case of all brokers down, terminate (will be restarted with exponential back-off)
				if dlqMsg.Code() == kafka.ErrAllBrokersDown {
					return actionShutdown
				}
			default:
				// Docs don't say if this could happen in single event produce.
				// Probably not, but if so it might only lead to duplicates, so keep warn log here.
				log.Warnf(e.lgprfx()+"unexpected Kafka info event inside DLQ producer loop, %v", dlqMsg)
			}
		}

		if err != nil {
			log.Errorf(e.lgprfx()+"failed (attempt #%d) to publish event (%+v) on DLQ topic '%s' with err: %v - next attempt in %d seconds", i, m, e.dlqTopicName(), err, backoffDuration)
			time.Sleep(time.Duration(backoffDuration) * time.Second)

			if backoffDuration < dlqMaxPublishBackoffTimeSec {
				backoffDuration *= 2
			}
		}
	}
}
