package entity

import "context"

type HookAction int

const (
	HookActionInvalid          HookAction = iota // default, not to be used
	HookActionProceed                            // continue processing of this event
	HookActionSkip                               // skip processing of this event and take next
	HookActionRetryableError                     // let Geist handle this event as a retryable error
	HookActionUnretryableError                   // let Geist handle this event as an unretryable error (e.g. corrupt event to be sent to DLQ)
	HookActionShutdown                           // shut down this stream instance
)

// PreTransformHookFunc is a client-provided function which the stream's Executor use
// prior to sending the event to the Transfomer. This way the client could modifiy/enrich
// each event before being processed according to the transform part of the spec.
// Since errors in this func is solely part of the client domain there is no point in
// returning them to the Geist executor. It is up the the client to decide appropriate
// actions to take, including optionally returning one of the HookAction error values.
// The event is provided as a mutable argument to avoid requiring the client to always
// return data even if not used.
// The stream spec governing the provided event is provided for context and filtering
// logic capabilities, since the function is called for all concurrently running streams.
type PreTransformHookFunc func(ctx context.Context, spec *Spec, event *[]byte) HookAction

// PostTransformHookFunc serves the same purpose and functionality as the PreTransformHookFunc
// but is called after the event transformations.
type PostTransformHookFunc func(ctx context.Context, spec *Spec, event *[]*Transformed) HookAction
