package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/zpiroux/geist/entity"
)

// NewEmitterFactory provides a custom Extractor type that continuously emits events
// with increasing event ID values. It simulates the autonomous extractor types like
// Kafka and GCP Pubsub.
func NewEmitterFactory() entity.ExtractorFactory {
	return &emitterFactory{sourceId: "eventEmitter"}
}

type emitterFactory struct {
	sourceId string
}

func (ef *emitterFactory) SourceId() string {
	return ef.sourceId
}

func (ef *emitterFactory) NewExtractor(ctx context.Context, c entity.Config) (entity.Extractor, error) {
	customConfig := c.Spec.Source.Config.CustomConfig.(map[string]any)
	interval, _ := strconv.Atoi(customConfig["emitIntervalSeconds"].(string))
	return &emitter{spec: c.Spec, emitInterval: interval}, nil
}

func (ef *emitterFactory) Close(ctx context.Context) error {
	log.Println("[emitterFactory] Close() called")
	return nil
}

type emitter struct {
	spec         *entity.Spec
	emitInterval int
}

type Event struct {
	EventId   int    `json:"eventId"`
	Timestamp int    `json:"ts"`
	Info      string `json:"info"`
}

func (e *emitter) StreamExtract(
	ctx context.Context,
	reportEvent entity.ProcessEventFunc,
	err *error,
	retryable *bool) {

	var evt Event

	for {

		time.Sleep(time.Duration(e.emitInterval) * time.Second)

		// An Extractor should check if its context has been cancelled, e.g. due to service shutting down
		if ctx.Err() == context.Canceled {
			log.Println("context canceled in StreamExtract")
			*retryable = false
			return
		}

		evt.Timestamp = int(time.Now().UnixMilli()) // using millis to exemplify transform options
		evt.Info = "some info"
		evt.EventId += 1
		eventBytes, _ := json.Marshal(evt)

		result := reportEvent(ctx, []entity.Event{{Data: eventBytes}})
		*err = result.Error
		*retryable = result.Retryable

		switch result.Status {
		case entity.ExecutorStatusSuccessful:
			continue
		case entity.ExecutorStatusShutdown:
			log.Printf("shutting down, last event ID: %d", evt.EventId)
			return
		default:
			log.Printf("not handling special cases/errors in this example, ExecutorStatus = %v", result.Status)
		}
	}
}

// Unused Extractor functionality

func (se *emitter) Extract(ctx context.Context, query entity.ExtractorQuery, result any) (error, bool) {
	return nil, false
}
func (se *emitter) ExtractFromSink(ctx context.Context, query entity.ExtractorQuery, result *[]*entity.Transformed) (error, bool) {
	return nil, false
}
func (se *emitter) SendToSource(ctx context.Context, event any) (string, error) {
	return "", nil
}
