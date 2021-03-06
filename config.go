package geist

import (
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/internal/pkg/admin"
	"github.com/zpiroux/geist/internal/pkg/entity/channel"
	"github.com/zpiroux/geist/internal/pkg/entity/void"
	"github.com/zpiroux/geist/internal/service"
)

const (
	defaultMaxStreamRetryIntervalSec = 300
	defaultEventLogInterval          = 10000
)

// Config needs to be created with NewConfig() and filled in with config as applicable
// for the intended setup, and provided in the call to geist.New().
// All config fields are optional.
type Config struct {
	Registry    SpecRegistryConfig
	AdminStream AdminStreamConfig
	Hooks       HookConfig
	Ops         OpsConfig

	extractors entity.ExtractorFactories
	loaders    entity.LoaderFactories
}

// HookConfig enables a Geist client to inject custom logic to the stream processing, such as 
// enrichment, deduplication, and filtering (if existing spec transform options not suitable).
type HookConfig struct {
	PreTransformHookFunc entity.PreTransformHookFunc
}

// NewConfig returns an initialized Config struct, required for geist.New().
// With this config applicable Source/Sink extractors/loaders should be registered
// before calling geist.New().
func NewConfig() *Config {
	return &Config{
		Ops:        OpsConfig{EventLogInterval: defaultEventLogInterval, MaxStreamRetryIntervalSec: defaultMaxStreamRetryIntervalSec},
		extractors: make(entity.ExtractorFactories),
		loaders:    make(entity.LoaderFactories),
	}
}

// RegisterLoaderType is used to prepare config for Geist to make this particular 
// Sink/Loader type availble for stream specs to use. This can only be done after
// a geist.NewConfig() and prior to creating Geist with geist.New().
func (c *Config) RegisterLoaderType(loaderFactory entity.LoaderFactory) error {
	if _, ok := entity.ReservedEntityNames[loaderFactory.SinkId()]; ok {
		return ErrInvalidEntityId
	}
	c.registerLoaderType(loaderFactory)
	return nil
}

// RegisterExtractorType is used to prepare config for Geist to make this particular
// Source/Extractor type available for stream specs to use. This can only be done after
// a geist.NewConfig() and prior to creating Geist with geist.New().
func (c *Config) RegisterExtractorType(extractorFactory entity.ExtractorFactory) error {
	if _, ok := entity.ReservedEntityNames[extractorFactory.SourceId()]; ok {
		return ErrInvalidEntityId
	}
	c.registerExtractorType(extractorFactory)
	return nil
}

func (c *Config) registerLoaderType(loaderFactory entity.LoaderFactory) {
	c.loaders[loaderFactory.SinkId()] = loaderFactory
}

func (c *Config) registerExtractorType(extractorFactory entity.ExtractorFactory) {
	c.extractors[extractorFactory.SourceId()] = extractorFactory
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

// OpsConfig provide options to optimize run-time performance and resilience.
type OpsConfig struct {
	EventLogInterval          int
	MaxStreamRetryIntervalSec int
}

func preProcessConfig(config *Config) service.Config {

	// Register native loader/sink types
	config.registerExtractorType(channel.NewExtractorFactory())
	config.registerLoaderType(void.NewLoaderFactory())

	// Convert external config to internal
	var c service.Config
	c.AdminStreamSpec = config.AdminStream.StreamSpec
	c.Registry.StorageMode = toGeistStorageMode(config.Registry.StorageMode)
	c.Registry.RegSpec = config.Registry.StreamSpec
	c.Entity.Loaders = config.loaders
	c.Entity.Extractors = config.extractors
	c.Engine.EventLogInterval = config.Ops.EventLogInterval
	c.Engine.MaxStreamRetryIntervalSec = config.Ops.MaxStreamRetryIntervalSec
	c.Engine.PreTransformHookFunc = config.Hooks.PreTransformHookFunc

	return c
}

func toGeistStorageMode(mode string) admin.RegStorageMode {
	switch mode {
	case string(admin.RegStorageNative):
		return admin.RegStorageNative
	case string(admin.RegStorageInMemory):
		return admin.RegStorageInMemory
	case string(admin.RegStorageCustom):
		return admin.RegStorageCustom
	default:
		return admin.RegStorageInMemory
	}
}
