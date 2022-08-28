package engine

import (
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/internal/pkg/igeist"
)

type Config struct {
	RegSpec                   igeist.Spec                 // Built-in spec for GEIST spec registrations
	AdminSpec                 igeist.Spec                 // Built-in spec for GEIST admin event notifications
	PreTransformHookFunc      entity.PreTransformHookFunc `json:"-"`
	EventLogInterval          int
	MaxStreamRetryIntervalSec int
}
