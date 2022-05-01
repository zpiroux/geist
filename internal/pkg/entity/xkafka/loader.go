package xkafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/zpiroux/geist/internal/pkg/model"
)

const flushTimeoutSec = 10

type Loader struct {
	pf                            ProducerFactory
	producer                      Producer
	ac                            AdminClient
	config                        *Config
	id                            string
	eventCount                    int64
	requestShutdown               bool
	sm                            sync.Mutex // shutdown mutex
	shutdownDeliveryReportHandler context.CancelFunc
}

func NewLoader(ctx context.Context, config *Config, id string, pf ProducerFactory) (*Loader, error) {

	var err error
	if isNil(pf) {
		pf = DefaultProducerFactory{}
	}

	l := &Loader{
		pf:     pf,
		config: config,
		id:     id,
	}

	if config.sinkTopic == nil {
		return l, fmt.Errorf("no topic spec provided when creating loader: %+v", l)
	} else if config.sinkTopic.Name == "" {
		return l, fmt.Errorf("no topic name provided when creating loader: %+v", l)
	}

	if err = l.createProducer(); err != nil {
		return l, err
	}

	admcli, err := l.pf.NewAdminClientFromProducer(l.producer)
	if err != nil {
		return l, fmt.Errorf(l.lgprfx()+"couldn't create admin client, err: %v", err)
	}
	l.ac = admcli

	if err = l.createTopic(ctx, l.config.sinkTopic); err != nil {
		return l, err
	}

	if !l.config.synchronous {
		ctxDRH, cancel := context.WithCancel(ctx)
		if cancel == nil {
			return l, fmt.Errorf(l.lgprfx() + "cancel func for deliveryReportHandler could not be created")
		}
		l.shutdownDeliveryReportHandler = cancel
		go l.deliveryReportHandler(ctx, ctxDRH)
	}
	return l, nil
}

func (l *Loader) StreamLoad(ctx context.Context, data []*model.Transformed) (string, error, bool) {

	if l.requestShutdown {
		return "", model.ErrEntityShutdownRequested, false
	}

	if data[0] == nil {
		return "", errors.New("streamLoad called without data to load (data[0] == nil)"), false
	}

	payloadKey := l.config.spec.Sink.Config.Message.PayloadFromId

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &l.config.sinkTopic.Name, Partition: kafka.PartitionAny},
		Value:          data[0].Data[payloadKey].([]byte), // TODO: Get type from spec, to not require byte
	}

	return l.publishMessage(context.Background(), msg)
}

func (l *Loader) Shutdown() {
	l.sm.Lock()
	defer l.sm.Unlock()
	log.Infof(l.lgprfx() + "shutdown initiated")
	if l.producer != nil {
		if unflushed := l.producer.Flush(flushTimeoutSec * 1000); unflushed > 0 {
			log.Errorf(l.lgprfx() + "%d messages did not get flushed during shutdown, check for potential message loss")
		} else {
			log.Infof(l.lgprfx() + "all messages flushed")
		}
		if l.shutdownDeliveryReportHandler != nil {
			l.shutdownDeliveryReportHandler()
		}
		l.producer.Close()
		l.producer = nil
		log.Infof(l.lgprfx()+"shutdown completed, number of published events: %d", l.eventCount)
	}
}

func (l *Loader) createProducer() error {

	var err error
	kconfig := make(kafka.ConfigMap)
	for k, v := range l.config.configMap {
		kconfig[k] = v
	}

	l.producer, err = l.pf.NewProducer(&kconfig)

	if err != nil {
		return fmt.Errorf(l.lgprfx()+"Failed to create producer: %s", err.Error())
	}

	log.Infof(l.lgprfx()+"Created producer %+v with config: %s", l.producer, l.config)
	return nil

}

func (l *Loader) publishMessage(ctx context.Context, m *kafka.Message) (string, error, bool) {

	var (
		resourceId string // not assigned at the moment
		err        error
		retryable  bool
	)

	if l.config.spec.Ops.LogEventData {
		log.Debugf(l.lgprfx()+"sending event %+v with producer: %+v", string(m.Value), l.producer)
	}

	start := time.Now()
	err = l.producer.Produce(m, nil)

	if !l.config.synchronous {
		if l.config.spec.Ops.LogEventData {
			duration := time.Since(start)
			log.Infof(l.lgprfx()+"event enqueued async [duration: %v] with err: %v", duration, err)
		}
		return resourceId, err, true
	}

	if err == nil {
		event := <-l.producer.Events()
		switch msg := event.(type) {
		case *kafka.Message:
			if msg.TopicPartition.Error != nil {
				err = fmt.Errorf("publish failed with err: %v", msg.TopicPartition.Error)
				retryable = true
			} else {
				l.eventCount++
				if l.config.spec.Ops.LogEventData {
					duration := time.Since(start)
					log.Infof(l.lgprfx()+"event published [duration: %v] to %s [%d] at offset: %v, key: %v value: %s",
						duration, *msg.TopicPartition.Topic, msg.TopicPartition.Partition,
						msg.TopicPartition.Offset, string(msg.Key), string(msg.Value))
				}
			}
		case kafka.Error:
			err = fmt.Errorf(l.lgprfx()+"Kafka error in producer, code: %v, event: %v", msg.Code(), msg)
			// In case of all brokers down, terminate (will be restarted with exponential backoff)
			if msg.Code() == kafka.ErrAllBrokersDown {
				err = model.ErrEntityShutdownRequested
				retryable = false
			}
		default:
			// Docs don't say if this could happen in single event produce.
			// Probably not, but if so we don't know if Produce() succeeded, so need to retry
			err = fmt.Errorf(l.lgprfx()+"unexpected Kafka info event from Kafka Producer report: %v, treat as error and retry", msg)
			retryable = true
		}
	} else {
		log.Errorf(l.lgprfx()+"kafka.producer.Produce() failed with err: %v, msg: %+v, topic: %s", err, m, l.config.topics[0], err)
		retryable = true // Treat all these kinds of errors as retryable for now
	}

	return resourceId, err, retryable
}

func (l *Loader) deliveryReportHandler(ctxParent context.Context, ctxThis context.Context) {

	for {
		select {

		case <-ctxParent.Done():
			log.Infof(l.lgprfx() + "[DRH] parent ctx closed, requesting shutdown")
			l.requestShutdown = true

		case <-ctxThis.Done():
			log.Infof(l.lgprfx() + "[DRH] ctx closed, shutting down")
			return

		case e := <-l.producer.Events():
			switch event := e.(type) {
			case *kafka.Message:
				m := event
				if m.TopicPartition.Error != nil {
					log.Errorf(l.lgprfx()+"[DRH] publish failed with err: %v", m.TopicPartition.Error)
				} else {
					l.eventCount++
					if l.config.spec.Ops.LogEventData {
						log.Infof(l.lgprfx()+"[DRH] event published to %s [%d] at offset: %v, key: %v value: %s",
							*m.TopicPartition.Topic, m.TopicPartition.Partition,
							m.TopicPartition.Offset, string(m.Key), string(m.Value))
					}
				}

			case kafka.Error:
				e := event
				if e.IsFatal() {
					log.Errorf(l.lgprfx()+"[DRH] fatal error: %v, requesting shutdown", e)
					l.requestShutdown = true
				} else {
					log.Errorf(l.lgprfx()+"[DRH] error: %v", e)
				}

			default:
				log.Infof(l.lgprfx()+"[DRH] Ignored event: %s", event)
			}
		}
	}
}

func (l *Loader) createTopic(ctx context.Context, topicSpec *model.TopicSpecification) error {

	// Not really needed in current topic creation implementation, but keep for now
	l.config.topicCreationMutex.Lock()
	defer l.config.topicCreationMutex.Unlock()

	topic := kafka.TopicSpecification{
		Topic:             topicSpec.Name,
		NumPartitions:     topicSpec.NumPartitions,
		ReplicationFactor: topicSpec.ReplicationFactor,
		//Config:            topicSpec.Config, // TODO: add to test (if config needed in a future update)
	}

	res, err := l.ac.CreateTopics(ctx, []kafka.TopicSpecification{topic})

	if err == nil {
		log.Infof(l.lgprfx()+"topic created: %+v", res)
		return nil
	}

	if err.Error() == kafka.ErrTopicAlreadyExists.String() {
		log.Infof(l.lgprfx()+"topic %s for this stream already exists", topicSpec.Name)
		err = nil
	} else {
		log.Errorf(l.lgprfx()+"could not create topic with spec: %+v, err: %v", topic, err)
	}

	return err
}

func (l *Loader) lgprfx() string {
	return "[xkafka.loader:" + l.id + "] "
}
