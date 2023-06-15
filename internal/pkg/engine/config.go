package engine

import (
	"github.com/zpiroux/geist/entity"
)

type Config struct {
	RegSpec                   *entity.Spec                // Built-in spec for GEIST spec registrations
	AdminSpec                 *entity.Spec                // Built-in spec for GEIST admin event notifications
	PreTransformHookFunc      entity.PreTransformHookFunc `json:"-"`
	MaxStreamRetryIntervalSec int
	NotifyChan                entity.NotifyChan `json:"-"`
	Log                       bool
	EventLogInterval          int
}
