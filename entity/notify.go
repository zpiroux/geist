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

var notifyLevelName = map[int]string{
	NotifyLevelInvalid: "INVALID",
	NotifyLevelDebug:   "DEBUG",
	NotifyLevelInfo:    "INFO",
	NotifyLevelWarn:    "WARN",
	NotifyLevelError:   "ERROR",
}

func NotifyLevelName(notifyLevel int) string {
	name, ok := notifyLevelName[notifyLevel]
	if !ok {
		name = "INVALID"
	}
	return name
}
