package engine

import (
	"github.com/zpiroux/geist/internal/pkg/igeist"
)

type Config struct {
	RegSpec                   igeist.Spec // Built-in spec for GEIST spec registrations
	AdminSpec                 igeist.Spec // Built-in spec for GEIST admin event notifications
	EventLogInterval          int
	MaxStreamRetryIntervalSec int
}
