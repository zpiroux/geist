package notify

import (
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/teltech/logger"
	"github.com/zpiroux/geist/entity"
)

// Notifier provides a way to send notification/log events to both an externally accessible
// channel and to log framework.
type Notifier struct {
	ch          entity.NotifyChan
	log         *logger.Log
	callerLevel int
	sender      string
	instance    string
	stream      string
}

// New creates a new Notifier. For proper value on the caller func name, set `callerLevel` to:
//		1 - if the notifying func is immediately above the called Notify()
//		2 - if the notifying func is two levels above
//		... etc
func New(ch entity.NotifyChan, log *logger.Log, callerLevel int, sender, instance, stream string) *Notifier {
	return &Notifier{
		ch:          ch,
		log:         log,
		callerLevel: callerLevel,
		sender:      sender,
		instance:    instance,
		stream:      stream,
	}
}

// Notify sends the provided data to the provided channel (and optionally log framework),
// together with additional data depending on notification level:
//		DEBUG and INFO: name of calling func
//		WARN: as INFO plus file and line number
//		ERROR: as WARN plus the full stack trace.
func (n *Notifier) Notify(level int, message string, args ...any) {

	var streamPrefix, streamSuffix string

	msg := fmt.Sprintf(message, args...)
	event := entity.NotificationEvent{
		Sender:   n.sender,
		Instance: n.instance,
		Stream:   n.stream,
		Message:  msg,
	}

	n.SendNotificationEvent(level, event)

	if n.log == nil {
		return
	}

	if n.stream != "" {
		streamPrefix = "(stream: "
		streamSuffix = ")"
	}

	const fmtstr = "[%s:%s]%s%s%s %s"
	switch level {
	case entity.NotifyLevelDebug:
		n.log.Debugf(fmtstr, n.sender, n.instance, streamPrefix, n.stream, streamSuffix, msg)
	case entity.NotifyLevelInfo:
		n.log.Infof(fmtstr, n.sender, n.instance, streamPrefix, n.stream, streamSuffix, msg)
	case entity.NotifyLevelWarn:
		n.log.Warnf(fmtstr, n.sender, n.instance, streamPrefix, n.stream, streamSuffix, msg)
	case entity.NotifyLevelError:
		n.log.Errorf(fmtstr, n.sender, n.instance, streamPrefix, n.stream, streamSuffix, msg)
	}
}

// SendNotificationEvent takes a formatted NotificationEvent, enrich it with info
// such as func, file, line, call stack, and sends it to the channel.
func (n *Notifier) SendNotificationEvent(notifyLevel int, event entity.NotificationEvent) {

	var (
		pc             uintptr
		line           int
		file, funcName string
	)

	pc, file, line, _ = runtime.Caller(n.callerLevel)
	funcName = "unknown"
	f := runtime.FuncForPC(pc)
	if f != nil {
		_, funcName = filepath.Split(f.Name())
	}

	event.Level = entity.NotifyLevelName(notifyLevel)
	event.Func = funcName
	if event.Timestamp == "" {
		event.Timestamp = time.Now().UTC().Format("2006-01-02T15:04:05.000000Z")
	}

	if notifyLevel >= entity.NotifyLevelWarn {

		event.File = file
		event.Line = line
	}

	if notifyLevel == entity.NotifyLevelError {

		stackTrace := make([]byte, 1024)
		stackTrace = stackTrace[:runtime.Stack(stackTrace, false)]
		event.StackTrace = string(stackTrace)
	}

	select {
	case n.ch <- event:
	default:
	}
}
