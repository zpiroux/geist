package channel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/teltech/logger"
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/internal/pkg/admin"
)

var log *logger.Log

func init() {
	log = logger.New()
}

const sourceTypeId = "geistapi"

type ExtractorFactory struct {
}

func NewExtractorFactory() entity.ExtractorFactory {
	return &ExtractorFactory{}
}

func (lf *ExtractorFactory) SourceId() string {
	return sourceTypeId
}

func (lf *ExtractorFactory) NewExtractor(ctx context.Context, spec *entity.Spec, id string) (entity.Extractor, error) {
	return newExtractor(spec.Id(), id)
}

func (lf *ExtractorFactory) Close() error {
	return nil 
}

type extractor struct {
	id         string
	specId     string
	sourceChan admin.EventChannel
}

func newExtractor(specId string, id string) (*extractor, error) {

	var (
		err       error
		extractor extractor
	)
	extractor.id = id
	extractor.specId = specId
	extractor.sourceChan = make(admin.EventChannel)

	log.Debugf(extractor.lgprfx()+"Newextractor with specId: %+v", specId)
	return &extractor, err
}

func (e *extractor) StreamExtract(
	ctx context.Context,
	reportEvent entity.ProcessEventFunc,
	err *error,
	retryable *bool) {

	log.Debugf(e.lgprfx() + "Starting up StreamExtract")

	var event admin.ChanEvent

	for {
		select {

		case <-ctx.Done():
			*err = ctx.Err()
			log.Infof(e.lgprfx()+"ctx closed, ctx.Err: %v", *err)
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
			log.Debugf(e.lgprfx()+"Sending back result %+v to caller", extResult)

			event.ResultChannel <- extResult

			close(event.ResultChannel)
		}
	}
}

// TODO: Change eventData to []byte
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
	log.Debugf(e.lgprfx()+"Event sent in extractor with spec: %+v", e.specId)

	result := <-resultChan
	log.Debugf(e.lgprfx()+"Result received, result: %+v", result)

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
			result.Error = fmt.Errorf("unexpected error trying to send event to channel extractor (%+v)", e.specId)
		}
	}
	return result.Id, result.Error
}

func (e *extractor) lgprfx() string {
	return "[channel.extractor:" + e.id + "] "
}
