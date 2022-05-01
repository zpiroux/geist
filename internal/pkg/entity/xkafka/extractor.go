package xkafka

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/teltech/logger"
	"github.com/zpiroux/geist/internal/pkg/model"
)

type action int

const (
	actionContinue action = iota
	actionShutdown
)

var log *logger.Log

func init() {
	log = logger.New()
}

type Extractor struct {
	cf          ConsumerFactory
	pf          ProducerFactory
	consumer    Consumer
	dlqProducer Producer
	config      *Config
	ac          AdminClient
	id          string
	eventCount  int64
}

func NewExtractor(config *Config, id string) (*Extractor, error) {

	e := &Extractor{
		cf:     DefaultConsumerFactory{},
		pf:     DefaultProducerFactory{},
		config: config,
		id:     id,
	}
	if len(config.topics) == 0 {
		return e, fmt.Errorf("no topics provided when creating extractor: %+v", e)
	}
	log.Infof(e.lgprfx()+"extractor created with config: %s", e.config)
	return e, nil
}

func (e *Extractor) StreamExtract(
	ctx context.Context,
	reportEvent model.ProcessEventFunc,
	err *error,
	retryable *bool) {

	var (
		events []model.Event
		msgs   []*kafka.Message
	)

	log.Infof(e.lgprfx()+"stream extract starting up with ops: %+v, config %s", e.config.spec.Ops, e.config)
	defer e.closeStreamExtract()

	*retryable = true
	if *err = e.initStreamExtract(ctx); *err != nil {
		return
	}

	run := true
	microBatchStart := time.Now()
	microBatchBytes := 0

	for run {

		event := e.consumer.Poll(e.config.pollTimeoutMs)

		if ctx.Err() == context.Canceled {
			log.Info(e.lgprfx() + "context canceled in StreamExtract")
			*retryable = false
			return
		}

		if event == nil {

			if e.config.spec.Ops.MicroBatch {
				// Poll timeout events are used to check if the microbatch window has closed and therefore if
				// event batch should be sent to downstream processing.
				if len(events) > 0 && microBatchTimedOut(microBatchStart, e.config.spec.Ops.MicroBatchTimoutMs) {
					if e.config.spec.Ops.LogEventData {
						log.Infof(e.lgprfx()+"microbatch window timed out (started: %v), processing unfilled batch of %d events", microBatchStart, len(events))
					}
					switch e.handleEventProcessingResult(ctx, msgs, reportEvent(ctx, events), err, retryable) {
					case actionShutdown:
						run = false
					case actionContinue:
						e.eventCount += int64(len(events))
						msgs = nil
						events = nil
						microBatchStart = time.Now()
						microBatchBytes = 0
					}
				}
			}
			continue
		}

		switch evt := event.(type) {
		case *kafka.Message:
			if evt.TopicPartition.Error != nil {
				// This should be a producer-only error. Might have received this here long ago, so keep for logging purposes.
				*err = evt.TopicPartition.Error
				log.Errorf(e.lgprfx()+"topic partition error when consuming message, msg: %+v, msg value: %s, err: %s", evt, string(evt.Value), *err)

				// Using normal event processing for this undocumented behaviour
				// Alternatively, switch to 'continue' here, depending on test results.
			}
			if e.config.spec.Ops.LogEventData {
				log.Infof(e.lgprfx()+"Event consumed from %s:%s", evt.TopicPartition, string(evt.Value))
				if evt.Headers != nil {
					log.Infof(e.lgprfx()+"Headers: %v", evt.Headers)
				}
			}

			msgs = append(msgs, evt)

			if e.config.spec.Ops.MicroBatch {

				microBatchBytes += len(evt.Value)
				events = append(events, model.Event{
					Key:  evt.Key,
					Ts:   evt.Timestamp,
					Data: evt.Value,
				})
				if len(events) < e.config.spec.Ops.MicroBatchSize && microBatchBytes < e.config.spec.Ops.MicroBatchBytes {
					continue
				}
				if e.config.spec.Ops.LogEventData {
					log.Infof(e.lgprfx()+"microbatch full, nb events: %d, bytes: %d", len(events), microBatchBytes)
				}

			} else {
				events = []model.Event{{
					Key:  evt.Key,
					Ts:   evt.Timestamp,
					Data: evt.Value,
				}}
			}

			switch e.handleEventProcessingResult(ctx, msgs, reportEvent(ctx, events), err, retryable) {

			case actionShutdown:
				run = false
			case actionContinue:
				e.eventCount += int64(len(events))
				msgs = nil
				events = nil
				microBatchStart = time.Now()
				microBatchBytes = 0
				continue
			}

		case kafka.Error:
			str := fmt.Sprintf("(%s) Kafka error in consumer, code: %v, event: %v", e.config.spec.Id(), evt.Code(), evt)
			log.Warnf(e.lgprfx() + str) // Most errors are recoverable

			// In case of all brokers down, terminate the extractor and let Executor/Supervisor decide what to do.
			if evt.Code() == kafka.ErrAllBrokersDown {
				*err = errors.New(str)
				run = false
			}
		default:
			if strings.Contains(evt.String(), "OffsetsCommitted") {
				if e.config.spec.Ops.LogEventData {
					log.Debugf(e.lgprfx()+"Kafka info event in consumer: %v", evt)
				}
			} else {
				log.Infof(e.lgprfx()+"Kafka info event in consumer: %v", evt)
			}
		}
	}
}

func (e *Extractor) handleEventProcessingResult(
	ctx context.Context,
	msgs []*kafka.Message,
	result model.EventProcessingResult,
	err *error,
	retryable *bool) action {

	switch result.Status {

	case model.ExecutorStatusSuccessful:
		if result.Error != nil {
			log.Errorf(e.lgprfx()+"bug in executor, shutting down, result.Error should be nil if ExecutorStatusSuccessful, result: %+v", result)
			return actionShutdown
		}
		*err = e.storeOffsets(msgs)
		return actionContinue

	case model.ExecutorStatusShutdown:
		log.Warnf(e.lgprfx()+"shutting down extractor due to executor shutdown, reportEvent result: %+v", result)
		return actionShutdown

	case model.ExecutorStatusRetriesExhausted:
		*err = fmt.Errorf(e.lgprfx()+"executor failed all retries, shutting down extractor, handing over to executor, reportEvent result: %+v", result)
		return actionShutdown

	case model.ExecutorStatusError:
		*retryable = false
		if result.Retryable {
			*err = fmt.Errorf(e.lgprfx() + "bug, executor should handle all retryable errors, until retries exhausted, shutting down extractor")
			return actionShutdown
		}
		str := e.lgprfx() + "executor had an unretryable error with this "
		if len(msgs) == 1 {
			str = fmt.Sprintf("%s"+"event: '%s', reportEvent result: %+v", str, string(msgs[0].Value), result)
		} else {
			str = fmt.Sprintf("%s"+"event batch, reportEvent result: %+v", str, result)
		}
		log.Warn(str)

		switch e.config.spec.Ops.HandlingOfUnretryableEvents {

		case model.HoueDefault:
			fallthrough
		case model.HoueDiscard:
			log.Warnf(e.lgprfx()+"a Kafka event failed downstream processing with result %+v; "+
				" since this stream (%s) does not have DLQ enabled, the event will now be discarded, events: %+v",
				result, e.config.spec.Id(), msgs)
			*err = e.storeOffsets(msgs)
			return actionContinue

		case model.HoueDlq:
			return e.moveEventsToDLQ(ctx, msgs)

		case model.HoueFail:
			str += " - since this stream's houe mode is set to HoueFail, the stream will now be shut down, requiring manual/external restart"
			*err = errors.New(str)
			return actionShutdown
		}
	}
	*err = fmt.Errorf("encountered a 'should not happen' error in Extractor.handleEventProcessingResult, "+
		"shutting down stream, reportEvent result %+v, events: %v, spec: %v", result, msgs, e.config.spec)
	*retryable = false
	return actionShutdown
}

func (e *Extractor) initStreamExtract(ctx context.Context) error {
	var err error

	if err = e.createConsumer(e.cf); err != nil {
		return err
	}

	admcli, err := e.cf.NewAdminClientFromConsumer(e.consumer)
	if err != nil {
		return fmt.Errorf(e.lgprfx()+"couldn't create admin client, err: %v", err)
	}
	e.ac = admcli

	if err = e.createDlqTopic(ctx, e.dlqTopicName()); err != nil {
		return err
	}
	if err = e.createDlqProducer(e.pf); err != nil {
		return err
	}
	if err = e.consumer.SubscribeTopics(e.config.topics, nil); err != nil {
		return fmt.Errorf(e.lgprfx()+"failed subscribing to topics '%v' with err: %v", e.config.topics, err)
	}
	return nil
}

func (e *Extractor) closeStreamExtract() {
	if !isNil(e.consumer) {
		log.Infof(e.lgprfx()+"closing Kafka consumer %+v, consumed events: %d", e.consumer, e.eventCount)
		if err := e.consumer.Close(); err != nil {
			log.Errorf(e.lgprfx()+"error closing Kafka consumer, err: %v", err)
		} else {
			log.Infof(e.lgprfx()+"Kafka consumer %+v closed successfully", e.consumer)
		}
	}
	if !isNil(e.dlqProducer) {
		e.dlqProducer.Close()
	}
	log.Infof(e.lgprfx() + "terminated")
}

func (e *Extractor) Extract(
	ctx context.Context,
	query model.ExtractorQuery,
	result any) (error, bool) {

	// Specific Kafka queries, topic lists, etc, not yet supported
	return errors.New("not supported"), false
}

func (e *Extractor) ExtractFromSink(
	ctx context.Context,
	query model.ExtractorQuery,
	result *[]*model.Transformed) (error, bool) {

	return errors.New("not supported"), false
}

func (e *Extractor) SendToSource(ctx context.Context, eventData any) (string, error) {
	log.Error(e.lgprfx() + "not applicable")
	return "", nil
}

func (e *Extractor) SetConsumerFactory(cf ConsumerFactory) {
	e.cf = cf
}

func (e *Extractor) SetProducerFactory(pf ProducerFactory) {
	e.pf = pf
}

func (e *Extractor) createConsumer(cf ConsumerFactory) error {

	kconfig := make(kafka.ConfigMap)
	for k, v := range e.config.configMap {
		kconfig[k] = v
	}

	consumer, err := cf.NewConsumer(&kconfig)

	if err != nil {
		return fmt.Errorf(e.lgprfx()+"failed to create consumer, config: %+v, err: %v", kconfig, err.Error())
	}

	e.consumer = consumer

	log.Infof(e.lgprfx()+"Created consumer %+v with config: %s", consumer, e.config)
	return nil
}

func (e *Extractor) storeOffsets(msgs []*kafka.Message) error {

	var offsets []kafka.TopicPartition

	for _, m := range msgs {
		tp := m.TopicPartition
		tp.Offset++
		offsets = append(offsets, tp)
	}

	if e.config.spec.Ops.LogEventData {
		log.Debugf(e.lgprfx()+"storing offsets for events: %v, offsets: %v", msgs, offsets)
	}
	offsets, err := e.consumer.StoreOffsets(offsets)

	if err != nil {
		// Should "never" happen since it's an in-mem operation. If it does (e.g. due to some app/client bug) there
		// is no point retrying, and no run-time fix, so just log error. Will in worst case cause duplicates, no loss.
		log.Errorf(e.lgprfx()+"error storing offsets, event: %+v, tp: %v, err: %v", msgs, offsets, err)
	}
	return err
}

func (e *Extractor) lgprfx() string {
	return "[xkafka.extractor:" + e.id + "] "
}

func microBatchTimedOut(start time.Time, mbTimeoutMs int) bool {
	return time.Since(start) > time.Duration(mbTimeoutMs)*time.Millisecond
}

func isNil(v any) bool {
	return v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil())
}
