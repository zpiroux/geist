package channel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/teltech/logger"
	"github.com/zpiroux/geist/internal/pkg/model"
)

var log *logger.Log

func init() {
	log = logger.New()
}

type Extractor struct {
	id         string
	specId     string
	sourceChan model.EventChannel
}

func NewExtractor(specId string, id string) (*Extractor, error) {

	var (
		err       error
		extractor Extractor
	)
	extractor.id = id
	extractor.specId = specId
	extractor.sourceChan = make(model.EventChannel)

	log.Debugf(extractor.lgprfx()+"NewExtractor with specId: %+v", specId)
	return &extractor, err
}

func (e *Extractor) StreamExtract(
	ctx context.Context,
	reportEvent model.ProcessEventFunc,
	err *error,
	retryable *bool) {

	log.Debugf(e.lgprfx() + "Starting up StreamExtract")

	var event model.ChanEvent

	for {
		select {

		case <-ctx.Done():
			*err = ctx.Err()
			log.Infof(e.lgprfx()+"ctx closed, ctx.Err: %v", *err)
			return

		case event = <-e.sourceChan:
			success := false
			result := reportEvent(ctx, []model.Event{{
				Data: event.Event.([]byte),
				Ts:   time.Now(),
				Key:  nil,
			}})
			if result.Error == nil {
				success = true
			}
			extResult := model.ResultChanEvent{
				Id:      result.ResourceId,
				Success: success,
				Error:   result.Error,
			}
			log.Debugf(e.lgprfx()+"Sending back result %+v to caller", extResult)

			event.ResultChannel <- extResult

			close(event.ResultChannel)
		}
	}
}

// TODO: Change eventData to []byte
func (e *Extractor) SendToSource(ctx context.Context, eventData any) (string, error) {

	resultChan := make(chan model.ResultChanEvent)
	event := model.ChanEvent{
		Event:         eventData,
		ResultChannel: resultChan,
	}

	if e.sourceChan == nil {
		return "", errors.New("bug, source chan must not be nil")
	}

	e.sourceChan <- event
	log.Debugf(e.lgprfx()+"Event sent in extractor with spec: %+v", e.specId)

	result := <-resultChan
	log.Debugf(e.lgprfx()+"Result received, result: %+v", result)

	return e.adjustToExternalErrors(result)
}

func (e *Extractor) Extract(ctx context.Context, query model.ExtractorQuery, result any) (error, bool) {
	return errors.New("not applicable"), false
}

func (e *Extractor) ExtractFromSink(ctx context.Context, query model.ExtractorQuery, result *[]*model.Transformed) (error, bool) {
	return errors.New("not applicable"), false
}

// To be 110% sure we're only responding with success, when event published correctly, in case
// of very rare (or potentially impossible) cases of replying with default values.
func (e *Extractor) adjustToExternalErrors(result model.ResultChanEvent) (string, error) {
	if result.Success {
		result.Error = nil
	} else {
		if result.Error == nil {
			// This should never (tm) happen, unless some weird unknown anomaly occurs.
			// But if it does happen, it will be handled correctly by applying an error explicitly, like below.
			result.Error = fmt.Errorf("unexpected error trying to send event to channel extractor (%+v)", e.specId)
		}
	}
	return result.Id, result.Error
}

func (e *Extractor) lgprfx() string {
	return "[channel.extractor:" + e.id + "] "
}
