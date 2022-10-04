package channel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/internal/pkg/admin"
)

const sourceTypeId = "geistapi"

type ExtractorFactory struct {
}

func NewExtractorFactory() entity.ExtractorFactory {
	return &ExtractorFactory{}
}

func (lf *ExtractorFactory) SourceId() string {
	return sourceTypeId
}

func (lf *ExtractorFactory) NewExtractor(ctx context.Context, c entity.Config) (entity.Extractor, error) {
	return newExtractor(c), nil
}

func (lf *ExtractorFactory) Close() error {
	return nil
}

type extractor struct {
	c          entity.Config
	sourceChan admin.EventChannel
}

func newExtractor(c entity.Config) *extractor {
	return &extractor{
		c:          c,
		sourceChan: make(admin.EventChannel),
	}
}

func (e *extractor) StreamExtract(
	ctx context.Context,
	reportEvent entity.ProcessEventFunc,
	err *error,
	retryable *bool) {

	var event admin.ChanEvent

	for {
		select {

		case <-ctx.Done():
			return

		case event = <-e.sourceChan:
			success := false
			result := reportEvent(ctx, []entity.Event{{
				Data: event.Event.([]byte),
				Ts:   time.Now(),
				Key:  nil,
			}})
			if result.Error == nil {
				success = true
			}
			extResult := admin.ResultChanEvent{
				Id:      result.ResourceId,
				Success: success,
				Error:   result.Error,
			}

			event.ResultChannel <- extResult
			close(event.ResultChannel)
		}
	}
}

func (e *extractor) SendToSource(ctx context.Context, eventData any) (string, error) {

	resultChan := make(chan admin.ResultChanEvent)
	event := admin.ChanEvent{
		Event:         eventData,
		ResultChannel: resultChan,
	}

	if e.sourceChan == nil {
		return "", errors.New("bug, source chan must not be nil")
	}

	e.sourceChan <- event
	result := <-resultChan

	return e.adjustToExternalErrors(result)
}

func (e *extractor) Extract(ctx context.Context, query entity.ExtractorQuery, result any) (error, bool) {
	return errors.New("not applicable"), false
}

func (e *extractor) ExtractFromSink(ctx context.Context, query entity.ExtractorQuery, result *[]*entity.Transformed) (error, bool) {
	return errors.New("not applicable"), false
}

// To be 110% sure we're only responding with success, when event published correctly, in case
// of very rare (or potentially impossible) cases of replying with default values.
func (e *extractor) adjustToExternalErrors(result admin.ResultChanEvent) (string, error) {
	if result.Success {
		result.Error = nil
	} else {
		if result.Error == nil {
			// This should never (tm) happen, unless some weird unknown anomaly occurs.
			// But if it does happen, it will be handled correctly by applying an error explicitly, like below.
			result.Error = fmt.Errorf("unexpected error trying to send event to channel extractor (streamId: %s)", e.c.Spec.Id())
		}
	}
	return result.Id, result.Error
}
