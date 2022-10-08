package entity

// NoficationEvent is the type of the events sent by Geist to the notification channel,
// which is accessible externally with geist.NotificationChannel().
type NotificationEvent struct {

	// The nofication level
	Level string

	// Timestamp of the event on the format "2006-01-02T15:04:05.000000Z"
	Timestamp string

	// The entity type of the sender, e.g. "executor", "supervisor", etc
	Sender string

	// The unique instance ID of the sender
	Instance string

	// The stream ID, if applicable
	Stream string

	Message string

	// Location and stack info, from where notification was sent.
	// Func is always provided.
	// File and Line are added when notification level is WARN or above.
	// StackTrace is added when notification level is ERROR.
	Func       string
	File       string
	Line       int
	StackTrace string
}

type NotifyChan chan NotificationEvent

const (
	NotifyLevelInvalid = iota
	NotifyLevelDebug
	NotifyLevelInfo
	NotifyLevelWarn
	NotifyLevelError
)

const (
	NotifyLevelStrInvalid = "INVALID"
	NotifyLevelStrDebug   = "DEBUG"
	NotifyLevelStrInfo    = "INFO"
	NotifyLevelStrWarn    = "WARN"
	NotifyLevelStrError   = "ERROR"
)

func NotifyLevelName(notifyLevel int) string {
	name, ok := notifyLevelName[notifyLevel]
	if !ok {
		name = NotifyLevelStrInvalid
	}
	return name
}

func NotifyLevel(notifyLevelName string) int {
	level, ok := notifyLevel[notifyLevelName]
	if !ok {
		level = NotifyLevelInvalid
	}
	return level
}

var notifyLevelName = map[int]string{
	NotifyLevelInvalid: NotifyLevelStrInvalid,
	NotifyLevelDebug:   NotifyLevelStrDebug,
	NotifyLevelInfo:    NotifyLevelStrInfo,
	NotifyLevelWarn:    NotifyLevelStrWarn,
	NotifyLevelError:   NotifyLevelStrError,
}

var notifyLevel = map[string]int{
	NotifyLevelStrInvalid: NotifyLevelInvalid,
	NotifyLevelStrDebug:   NotifyLevelDebug,
	NotifyLevelStrInfo:    NotifyLevelInfo,
	NotifyLevelStrWarn:    NotifyLevelWarn,
	NotifyLevelStrError:   NotifyLevelError,
}
