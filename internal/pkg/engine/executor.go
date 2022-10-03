package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/teltech/logger"
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/internal/pkg/igeist"
	"github.com/zpiroux/geist/internal/pkg/notify"
)

const (
	defaultInitialStreamExtractRetryBackoffDuration = 4
	defaultInitialStreamLoadRetryBackoffDuration    = 2
	defaultEventLogInterval                         = 500
	defaultMaxStreamRetryIntervalSec                = 240
)

var (
	ErrHookUnretryableError = errors.New("PreTransfromHookFunc reported unretryable error")
	ErrHookInvalidAction    = errors.New("PreTransfromHookFunc returned invalid action value")
)

// Stream Executors operates an ETL stream, from Source to Transform to Sink, as specified by
// a single GEIST stream spec. The stream it is executing is configured and instantiated by the Supervisor.
type Executor struct {
	config             Config
	stream             igeist.Stream
	ctx                context.Context    // Child ctx for shutting down Extractor
	cancel             context.CancelFunc // CancelFunc for shutting down Extractor
	id                 string
	notifier           *notify.Notifier
	shutdownInProgress bool // TODO: not important but add mutex on this

	// Number of events processed. Although we could use uint64 here, we can still have
	// each executor running almost 3 million years (assuming 100000 events/second) using
	// int64, before overflowing, so should be enough :)
	events int64
}

func NewExecutor(config Config, stream igeist.Stream) *Executor {

	e := &Executor{
		config: config,
		stream: stream,
		id:     stream.Instance(),
	}
	if e.config.EventLogInterval == 0 {
		e.config.EventLogInterval = defaultEventLogInterval
	}
	if e.config.MaxStreamRetryIntervalSec == 0 {
		e.config.MaxStreamRetryIntervalSec = defaultMaxStreamRetryIntervalSec
	}

	var log *logger.Log
	if config.Log {
		log = logger.New()
	}
	e.notifier = notify.New(config.NotifyChan, log, 2, "executor", e.id, e.StreamId())

	if e.valid() {
		return e
	}
	return nil
}

func (e *Executor) valid() bool {
	if e.stream == nil {
		return false
	}
	return e.stream.Spec() != nil &&
		e.stream.Extractor() != nil &&
		e.stream.Transformer() != nil &&
		e.stream.Loader() != nil
}

func (e *Executor) StreamId() string {
	return e.stream.Spec().Id()
}

func (e *Executor) Stream() igeist.Stream {
	return e.stream
}

func (e *Executor) Run(ctx context.Context, wg *sync.WaitGroup) {
	var (
		err       error
		retryable bool
	)

	e.ctx, e.cancel = context.WithCancel(ctx)
	defer e.runExit(wg)
	e.notifier.Notify(entity.NotifyLevelInfo, "Starting up")

	// Infinite retries with exponential backoff interval, for max self-healing when having retryable errors.
	// For unretryable errors it depends on the stream's config value Ops.HandlingOfUnretryableEvents.
	// If that is set to HoueDlq or HoueDiscard, the Extractor.StreamExtract will take care of those internally.
	// If set to HoueFail the loop will exit and extractor terminate. Stream needs to be restarted manually
	// (or at least externally). For other details on Houe modes, see entity.Spec.Ops.
	backoffDuration := defaultInitialStreamExtractRetryBackoffDuration
	for i := 0; ; i++ {

		e.stream.Extractor().StreamExtract(e.ctx, e.ProcessEvent, &err, &retryable)

		if err != nil && ctx.Err() != context.Canceled {
			e.notifier.Notify(entity.NotifyLevelError, "StreamExtract returned with error: %s, retryable: %v", err.Error(), retryable)
			if retryable {
				e.notifier.Notify(entity.NotifyLevelWarn, "stream restart (#%d) in %d seconds", i, backoffDuration)
				if !sleepCtx(ctx, time.Duration(backoffDuration)*time.Second) {
					break
				}
				if backoffDuration < e.config.MaxStreamRetryIntervalSec {
					backoffDuration *= 2
				}
				continue
			}
		}
		break
	}

	e.notifier.Notify(entity.NotifyLevelInfo, "finished. Events processed: %d", e.events)
}

func (e *Executor) runExit(wg *sync.WaitGroup) {
	// Protection against badly written extractor/source plugins
	if r := recover(); r != nil {
		e.notifier.Notify(entity.NotifyLevelError, "panic (%v) in StreamExtract() for spec %s, terminating stream", r, e.stream.Spec().JSON())
	}
	wg.Done()
}

// ProcessEvent is called by Extractor when event extracted from source.
// This design is chosen instead of a channel based one, to ensure efficient and reliable offset commit/pubsub ack
// only when sink success is ensured. It also reduces transloading latency to a minimum.
// TODO: Add better description and usage of the key parameter (it is currently sent by Kafka Extractors,
// as the message key).
// If event processing is successful result.Error will be nil, and result.Status will be set to ExecutorStatusSuccessful.
// If executor is shutting down, result.Error will be non-nil and result.Status will be set to ExecutorStatusShuttingDown
func (e *Executor) ProcessEvent(ctx context.Context, events []entity.Event) entity.EventProcessingResult {

	var (
		result      = entity.EventProcessingResult{Status: entity.ExecutorStatusError}
		transformed []*entity.Transformed
	)

	defer e.processEventExit()

	if e.shutdownInProgress {
		e.notifier.Notify(entity.NotifyLevelWarn, "rejecting event processing due to shutdown in progress, rejected events: %v", events)
		result.Error = nil
		result.Status = entity.ExecutorStatusShutdown
		return result
	}

	for _, event := range events {
		atomic.AddInt64(&e.events, 1)
		if e.events%int64(e.config.EventLogInterval) == 0 {
			e.notifier.Notify(entity.NotifyLevelInfo, "[metric] nb events processed: %d", e.events)
		}

		// Apply injection of stream processing logic if requested
		if e.config.PreTransformHookFunc != nil {

			action := e.config.PreTransformHookFunc(ctx, e.stream.Spec().Id(), &event.Data)

			switch action {
			case entity.HookActionProceed:
				// event processing to continue as normal
			case (entity.HookActionSkip):
				result.Status = entity.ExecutorStatusSuccessful
				result.Error = nil
				result.Retryable = false
				return result
			case (entity.HookActionUnretryableError):
				result.Status = entity.ExecutorStatusError
				result.Error = ErrHookUnretryableError
				result.Retryable = false
				return result
			case (entity.HookActionShutdown):
				result.Status = entity.ExecutorStatusShutdown
				return result
			default:
				result.Status = entity.ExecutorStatusError
				result.Error = fmt.Errorf("%w : %v", ErrHookInvalidAction, action)
				result.Retryable = false
				return result
			}
		}

		result.Retryable = true
		var transEvent []*entity.Transformed
		transEvent, result.Error = e.stream.Transformer().Transform(ctx, event.Data, &result.Retryable)
		if result.Error != nil {
			return result
		}
		if e.logEventData() {
			e.notifier.Notify(entity.NotifyLevelDebug, "event transformed into: %v", transEvent)
		}

		transformed = append(transformed, transEvent...)
	}

	if len(transformed) == 0 {
		result.Status = entity.ExecutorStatusSuccessful
		return result
	}

	return e.loadToSink(ctx, transformed)
}

func (e *Executor) loadToSink(ctx context.Context, transformed []*entity.Transformed) (result entity.EventProcessingResult) {
	loadAttempts := 0
	backoffDuration := defaultInitialStreamLoadRetryBackoffDuration
	result.Status = entity.ExecutorStatusError

	for ; loadAttempts <= e.maxRetryAttempts(); loadAttempts++ {

		result.ResourceId, result.Error, result.Retryable = e.stream.Loader().StreamLoad(ctx, transformed)

		if result.Error == nil {
			result.Status = entity.ExecutorStatusSuccessful
			break
		}

		if e.shuttingDown(ctx, result) {
			result.Status = entity.ExecutorStatusShutdown
			return result
		}

		if result.Retryable && loadAttempts < e.maxRetryAttempts() {

			e.notifier.Notify(entity.NotifyLevelWarn, "StreamLoad() failed with error: %v, issuing retry attempt #%d, in %d seconds", result.Error, loadAttempts+1, backoffDuration)
			if !sleepCtx(ctx, time.Duration(backoffDuration)*time.Second) {
				break
			}
			e.notifier.Notify(entity.NotifyLevelInfo, "issuing retry attempt #%d, after backoff %d seconds", loadAttempts+1, backoffDuration)
			backoffDuration = 2 * backoffDuration
			continue
		} else {
			// It's either a non-retryable error, or a retryable one exceeding retry limit, both of which should
			// terminate the retry loop
			break
		}
	}

	if result.Error != nil && result.Retryable {
		e.notifier.Notify(entity.NotifyLevelError, "giving up retrying load to sink for spec ID %s, after %d attempts, transformed event(s): %+v", e.StreamId(), loadAttempts, transformed)
		result.Status = entity.ExecutorStatusRetriesExhausted
		// From here, it's up to each extractor to handle DLQ logic
	}

	return result
}

func (e *Executor) shuttingDown(ctx context.Context, result entity.EventProcessingResult) bool {
	if ctx.Err() == context.Canceled {
		e.notifier.Notify(entity.NotifyLevelInfo, "context canceled during StreamLoad, err: %v", result.Error)
		return true
	}

	if result.Error == entity.ErrEntityShutdownRequested {
		e.notifier.Notify(entity.NotifyLevelInfo, "loader requested shutdown during StreamLoad")
		return true
	}
	return false
}

func (e *Executor) processEventExit() {
	// Protection against badly written loader/sink plugins or external hook logic
	if r := recover(); r != nil {
		e.notifier.Notify(entity.NotifyLevelError, "panic (%v) in ProcessEvent() for spec %s", r, e.stream.Spec().JSON())
	}
}

func (e *Executor) Shutdown() {
	e.shutdownInProgress = true // TODO: protect with mutex (not urgent)
	e.notifier.Notify(entity.NotifyLevelInfo, "Shutting down")

	// Shut down Extractor
	if e.cancel != nil {
		e.cancel()
	} else {
		e.notifier.Notify(entity.NotifyLevelWarn, "Shutdown request received before started running")
	}

	e.stream.Loader().Shutdown()
}

func (e *Executor) maxRetryAttempts() int {
	return e.stream.Spec().(*entity.Spec).Ops.MaxEventProcessingRetries
}

func (e *Executor) logEventData() bool {
	return e.stream.Spec().(*entity.Spec).Ops.LogEventData
}
