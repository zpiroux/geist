package geist

import (
	"github.com/zpiroux/geist/internal/pkg/model"
	geist "github.com/zpiroux/geist/internal/service"
)

const (
	defaultMaxStreamRetryIntervalSec = 300
	defaultEventLogInterval          = 10000
	defaultQueuedMaxMessagesKb       = 2048
	defaultPollTimeoutMs             = 3000
)

type Config struct {
	Registry    SpecRegistryConfig
	AdminStream AdminStreamConfig
	Ops         OpsConfig
	Gcp         GcpConfig
	Kafka       KafkaConfig // GEIST specs can override this
	BigTable    BigTableConfig
	BigQuery    BigQueryConfig
	Firestore   FirestoreConfig
	Pubsub      PubsubConfig
}

type GcpConfig struct {
	ProjectId string
}

// SpecRegistryConfig is only required to be filled in if persistance of specs is required.
// Normally only StorageMode field is needed for setting up intended behaviour, switching from
// in-mem storage to native persistance. Future updates might add more built-in "native" storage modes.
type SpecRegistryConfig struct {

	// StorageMode specifies how Geist should store stream specs to be run.
	// The follow values are available:
	//
	// 	"inmemory": (default) registered specs are stored in memory only
	// 	"native":   specs are persisted in the sink specified in the native stream spec for the
	//              spec registration stream (using Firestore as default sink)
	// 	"custom":   the client of Geist need to provide a Stream Spec for the Spec Registration Stream
	//              in the RegSpec field (advanced usage)
	StorageMode string

	// StreamSpec specifies the Stream Spec for the internal stream, handling Stream Spec registrations,
	// if StorageMode is set to "custom".
	StreamSpec []byte
}

// AdminStreamConfig specifies how the internal cross-pod admin event propagation should be set up.
// Providing a custom admin spec is advanced usage and should be done with care.
// It is only required to be filled in if diverting from default behavior, which is the following:
//
// * If SpecRegistryConfig.StorageMode is set to "inmemory" (default), the admin stream is disabled
//   (cross-pod sync not needed)
//
// * If SpecRegistryConfig.StorageMode is set to "native" (firestore) or "custom", the admin stream
//   will use GCP Pubsub for event propagation (pubsub set as its stream source type).
//
// For complete customization, set StorageMode to "custom" and provide both a Stream and an Admin Spec.
type AdminStreamConfig struct {
	StreamSpec []byte
}

type OpsConfig struct {
	Env                       string // Available values: "dev", "stage", "prod", "all"
	EventLogInterval          int    // TODO: change all the int types to string to understand if it's been set by client, and if not, apply an appropriate internal default value
	MaxStreamRetryIntervalSec int
}

type KafkaConfig struct {
	Enabled                  bool   // Set to true if Kafka used in any registered stream
	BootstrapServers         string // Default servers, can be overriden in GEIST specs
	ConfluentBootstrapServer string // Default server, can be overriden in GEIST specs
	ConfluentApiKey          string
	ConfluentApiSecret       string `json:"-"`
	PollTimeoutMs            int    // Default poll timeout, can be overridden in GEIST specs
	QueuedMaxMessagesKb      int    // Events consumed and processed before commit
}

type BigTableConfig struct {
	Enabled    bool // Set to true if Big Table used in any registered stream
	InstanceId string
}

type BigQueryConfig struct {
	Enabled bool // Set to true if BigQuery used in any registered stream
}

type FirestoreConfig struct {
	Enabled bool // Set to true if Firestore used in any registered stream, or if using "native" storage mode
}

type PubsubConfig struct {
	Enabled                bool // Set to true if Pubsub used in any registered stream
	MaxOutstandingMessages int
	MaxOutstandingBytes    int
}

func toInternalConfig(inConfig Config) geist.Config {
	var c geist.Config
	config := Config{
		Ops:   OpsConfig{EventLogInterval: defaultEventLogInterval, MaxStreamRetryIntervalSec: defaultMaxStreamRetryIntervalSec},
		Kafka: KafkaConfig{QueuedMaxMessagesKb: defaultQueuedMaxMessagesKb, PollTimeoutMs: defaultPollTimeoutMs},
	}
	config = inConfig
	c.ProjectId = config.Gcp.ProjectId
	c.AdminStreamSpec = config.AdminStream.StreamSpec
	c.Registry.StorageMode = toGeistStorageMode(config.Registry.StorageMode)
	c.Registry.RegSpec = config.Registry.StreamSpec
	c.Entity.Env = toGeistEnv(config.Ops.Env)
	c.Entity.Kafka.Enabled = config.Kafka.Enabled
	c.Entity.Kafka.BootstrapServers = config.Kafka.BootstrapServers
	c.Entity.Kafka.ConfluentBootstrapServer = config.Kafka.ConfluentBootstrapServer
	c.Entity.Kafka.ConfluentApiKey = config.Kafka.ConfluentApiKey
	c.Entity.Kafka.ConfluentApiSecret = config.Kafka.ConfluentApiSecret
	c.Entity.Kafka.PollTimeoutMs = config.Kafka.PollTimeoutMs
	c.Entity.Kafka.QueuedMaxMessagesKb = config.Kafka.QueuedMaxMessagesKb
	c.Entity.Pubsub.Enabled = config.Pubsub.Enabled
	c.Entity.Pubsub.MaxOutstandingMessages = config.Pubsub.MaxOutstandingMessages
	c.Entity.Pubsub.MaxOutstandingBytes = config.Pubsub.MaxOutstandingBytes
	c.Entity.BigTable.Enabled = config.BigTable.Enabled
	c.Entity.BigTable.InstanceId = config.BigTable.InstanceId
	c.Entity.BigQuery.Enabled = config.BigQuery.Enabled
	c.Entity.Firestore.Enabled = config.Firestore.Enabled
	c.Engine.EventLogInterval = config.Ops.EventLogInterval
	c.Engine.MaxStreamRetryIntervalSec = config.Ops.MaxStreamRetryIntervalSec
	return c
}

func toGeistEnv(env string) model.Environment {
	switch env {
	case string(model.EnvironmentDev):
		return model.EnvironmentDev
	case string(model.EnvironmentStage):
		return model.EnvironmentStage
	case string(model.EnvironmentProd):
		return model.EnvironmentProd
	case string(model.EnvironmentAll):
		return model.EnvironmentAll
	default:
		return model.EnvironmentAll
	}
}

func toGeistStorageMode(mode string) model.RegStorageMode {
	switch mode {
	case string(model.RegStorageNative):
		return model.RegStorageNative
	case string(model.RegStorageInMemory):
		return model.RegStorageInMemory
	case string(model.RegStorageCustom):
		return model.RegStorageCustom
	default:
		return model.RegStorageInMemory
	}
}
