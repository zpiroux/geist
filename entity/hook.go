package entity

import "context"

type HookAction int

const (
	HookActionInvalid          HookAction = iota // default, not to be used
	HookActionProceed                            // continue processing of this event
	HookActionSkip                               // skip processing of this event and take next
	HookActionUnretryableError                   // let Geist handle this event as an unretryable error (e.g. corrupt event to be sent to DLQ)
	HookActionShutdown                           // shut down this stream instance
)

// PreTransformHookFunc is a client-provided function which the stream's Executor use
// prior to sending the event to the Transfomer. This way the client could modifiy/enrich
// each event before being processed according to the transform part of the spec.
// Since errors in this func is solely part of the client domain there is no point in
// returning them to the Geist executor. It is up the the client to decide appropriate
// action to take.
// The event is provided as a mutable argument to avoid requiring the client to always
// return data even if not used.
type PreTransformHookFunc func(ctx context.Context, event *[]byte) HookAction
