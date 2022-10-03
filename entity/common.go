package entity

import (
	"errors"
)

// Native stream entity types (sources, sinks or both)
type EntityType string

const (
	EntityInvalid  EntityType = "invalid"
	EntityVoid     EntityType = "void"
	EntityAdmin    EntityType = "admin"
	EntityGeistApi EntityType = "geistapi"
)

var ReservedEntityNames = map[string]bool{
	string(EntityInvalid):  true,
	string(EntityVoid):     true,
	string(EntityAdmin):    true,
	string(EntityGeistApi): true,
}

// Config is the Entity Config to use with Entity factories
type Config struct {
	Spec       *Spec
	ID         string
	NotifyChan NotifyChan
	Log        bool
}

// Some Stream ETL Entities need different configurations based on environements.
// This is not possible to set in the generic GEIST build config since ETL entities are
// configured in externally provided ETL Stream Specs. The environment concept is
// therefore required to be known to the entity and to the stream spec.
//
// The following env types are provided by Geist for consistency across entity plugins,
// but any type of custom string can be used by plugin entities.
// For example, a custom plugin extractor could support having "env": "someregion-staging"
// in the stream spec using that extractor/source, since the extractor implementation can
// cast the Environment type back to string when matching.
type Environment string

const (
	EnvironmentAll   Environment = "all"
	EnvironmentDev   Environment = "dev"
	EnvironmentStage Environment = "stage"
	EnvironmentProd  Environment = "prod"
)

// An entity can request to be shut down. This error code should be returned and it's up to the
// Executor to decide if entire stream should be shutdown or any other action to be taken.
var ErrEntityShutdownRequested = errors.New("entity shutdown requested")
