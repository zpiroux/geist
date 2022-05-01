package model

import "errors"

// Available Stream ETL Entity Types (sources, sinks or both)
type EntityType string

const (
	EntityInvalid   EntityType = "invalid"
	EntityVoid      EntityType = "void"
	EntityAdmin     EntityType = "admin"
	EntityGeistApi  EntityType = "geistapi"
	EntityKafka     EntityType = "kafka"
	EntityPubsub    EntityType = "pubsub"
	EntityFirestore EntityType = "firestore"
	EntityBigTable  EntityType = "bigtable"
	EntityBigQuery  EntityType = "bigquery"
)

// Some Stream ETL Entities need different configurations based on environements.
// This is not possible to set in the generic GEIST build config since ETL entities are
// configured in externally provided ETL Stream Specs. The environment concept is
// therefore required to be known to the entity and to the stream spec.
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
