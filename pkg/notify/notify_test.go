package notify

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/entity"
)

const logLevelEnvName = "LOG_LEVEL"

func TestNotify(t *testing.T) {

	sender := "someSender"
	instance := "someId"
	stream := "someStreamId"
	expectedMessage := "some stuff happened, foo=11"
	fmtstr := "some stuff happened, foo=%d"
	fmtval := 11
	ch := make(entity.NotifyChan, 3)
	curLvl := os.Getenv(logLevelEnvName)
	os.Setenv(logLevelEnvName, entity.NotifyLevelStrDebug)

	notifier := New(ch, nil, 2, sender, instance, stream)

	// Test DEBUG
	notifier.Notify(entity.NotifyLevelDebug, fmtstr, fmtval)
	event := <-ch
	expectedEvent := entity.NotificationEvent{
		Level:    "DEBUG",
		Sender:   sender,
		Instance: instance,
		Stream:   stream,
		Message:  expectedMessage,
		Func:     "notify.TestNotify",
	}
	event.Timestamp = ""
	assert.Equal(t, expectedEvent, event)

	// Test INFO
	notifier.Notify(entity.NotifyLevelInfo, fmtstr, fmtval)
	event = <-ch
	expectedEvent.Level = "INFO"
	event.Timestamp = ""
	assert.Equal(t, expectedEvent, event)

	// Test WARN
	notifier.Notify(entity.NotifyLevelWarn, fmtstr, fmtval)
	event = <-ch
	expectedEvent.Level = "WARN"
	expectedEvent.File = "notify_test.go"
	expectedEvent.Line = 50
	event.Timestamp = ""
	event.File = filepath.Base(expectedEvent.File)
	assert.Equal(t, expectedEvent, event)

	// Test ERROR
	notifier.Notify(entity.NotifyLevelError, fmtstr, fmtval)
	event = <-ch
	expectedEvent.Level = "ERROR"
	expectedEvent.Line = 60
	event.Timestamp = ""
	event.File = filepath.Base(expectedEvent.File)
	assert.NotEmpty(t, event.StackTrace)
	event.StackTrace = ""
	assert.Equal(t, expectedEvent, event)

	os.Setenv(logLevelEnvName, curLvl)
}

func TestMinLogLevel(t *testing.T) {

	sender := "someSender"
	instance := "someId"
	stream := "someStreamId"
	ch := make(entity.NotifyChan, 3)
	curLvl := os.Getenv(logLevelEnvName)

	// Empty os env var --> min level INFO
	os.Setenv(logLevelEnvName, "")
	notifier := New(ch, nil, 2, sender, instance, stream)
	assert.Equal(t, entity.NotifyLevelInfo, notifier.minNotifyLevel)

	// Invalid os env var --> min level INFO
	os.Setenv(logLevelEnvName, "SOME_INVALID_LEVEL")
	notifier = New(ch, nil, 2, sender, instance, stream)
	assert.Equal(t, entity.NotifyLevelInfo, notifier.minNotifyLevel)

	// Valid levels
	os.Setenv(logLevelEnvName, entity.NotifyLevelStrInfo)
	notifier = New(ch, nil, 2, sender, instance, stream)
	assert.Equal(t, entity.NotifyLevelInfo, notifier.minNotifyLevel)

	os.Setenv(logLevelEnvName, entity.NotifyLevelStrWarn)
	notifier = New(ch, nil, 2, sender, instance, stream)
	assert.Equal(t, entity.NotifyLevelWarn, notifier.minNotifyLevel)

	os.Setenv(logLevelEnvName, entity.NotifyLevelStrError)
	notifier = New(ch, nil, 2, sender, instance, stream)
	assert.Equal(t, entity.NotifyLevelError, notifier.minNotifyLevel)

	os.Setenv(logLevelEnvName, curLvl)
}
