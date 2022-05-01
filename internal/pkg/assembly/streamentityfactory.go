package assembly

import (
	"context"
	"fmt"
	"sync"

	"github.com/zpiroux/geist/internal/pkg/entity/channel"
	"github.com/zpiroux/geist/internal/pkg/entity/transform"
	"github.com/zpiroux/geist/internal/pkg/entity/void"
	"github.com/zpiroux/geist/internal/pkg/entity/xbigquery"
	"github.com/zpiroux/geist/internal/pkg/entity/xbigtable"
	"github.com/zpiroux/geist/internal/pkg/entity/xfirestore"
	"github.com/zpiroux/geist/internal/pkg/entity/xkafka"
	"github.com/zpiroux/geist/internal/pkg/entity/xpubsub"
	"github.com/zpiroux/geist/internal/pkg/igeist"
	"github.com/zpiroux/geist/internal/pkg/model"
)

// StreamEntityFactory creates stream entities based on Stream Spec config and is a singleton,
// created by the Service, and operated by the StreamBuilder (also a singleton), which is given 
// to the Supervisor during creation.
type StreamEntityFactory struct {
	config      Config
	adminLoader igeist.Loader
}

// The mutexes here reduces the amount of unneeded requests for certain stream setup operations.
// If a stream is configured to operate with more than one concurrent instance (ops.streamsPerPod > 1),
// certain operations might be attempted by more than one of its stream entity instances (e.g. a stream's
// Kafka Extractors creating DLQ topics if requested in its spec).
// The mutex scope is per pod, but this is good enough in this case.
var (
	kafkaTopicCreationMutex sync.Mutex
	bigQueryMetadataMutex   sync.Mutex
)

func NewStreamEntityFactory(config Config) *StreamEntityFactory {

	return &StreamEntityFactory{config: config}
}

func (s *StreamEntityFactory) SetAdminLoader(loader igeist.Loader) {
	s.adminLoader = loader
}

func (s *StreamEntityFactory) CreateExtractor(ctx context.Context, etlSpec igeist.Spec, instanceId string) (igeist.Extractor, error) {

	spec := etlSpec.(*model.Spec)
	switch spec.Source.Type {

	case model.EntityKafka:
		return xkafka.NewExtractor(s.createKafkaExtractorConfig(spec), instanceId)

	case model.EntityPubsub:
		return xpubsub.NewExtractor(ctx, s.createPubsubExtractorConfig(spec), instanceId)

	case model.EntityGeistApi:
		return channel.NewExtractor(spec.Id(), instanceId)

	default:
		return nil, fmt.Errorf("could not create extractor, source type '%s' not implemented, spec: %+v", spec.Source.Type, spec)
	}
}

func (s *StreamEntityFactory) CreateSinkExtractor(ctx context.Context, etlSpec igeist.Spec, instanceId string) (igeist.Extractor, error) {

	spec := etlSpec.(*model.Spec)
	switch spec.Sink.Type {

	case model.EntityFirestore:
		return xfirestore.NewExtractor(*spec, instanceId, s.config.Firestore.Client, spec.Namespace)

	case model.EntityBigTable:
		return xbigtable.NewExtractor(*spec, s.config.BigTable.Client, s.config.BigTable.AdminClient)

	default:
		// This is not an error since *sinks* are not required to provide an extractor.
		// It's only the extractor for the *source* that is required.
		return nil, nil
	}
}

func (s *StreamEntityFactory) CreateTransformer(ctx context.Context, etlSpec igeist.Spec) (igeist.Transformer, error) {

	// Currently only supporting native GEIST Transformations
	spec := etlSpec.(*model.Spec)
	switch spec.Transform.ImplId {
	default:
		return transform.NewTransformer(spec), nil
	}
}

func (s *StreamEntityFactory) CreateLoader(ctx context.Context, etlSpec igeist.Spec, instanceId string) (igeist.Loader, error) {

	spec := etlSpec.(*model.Spec)
	switch spec.Sink.Type {

	case model.EntityKafka:
		return xkafka.NewLoader(ctx, s.createKafkaLoaderConfig(spec), instanceId, nil)

	case model.EntityBigTable:
		return xbigtable.NewLoader(ctx, spec, instanceId, s.config.BigTable.Client, s.config.BigTable.AdminClient)

	case model.EntityBigQuery:
		return xbigquery.NewLoader(
			ctx,
			spec,
			instanceId,
			xbigquery.NewBigQueryClient(instanceId, s.config.BigQuery.Client),
			&bigQueryMetadataMutex)

	case model.EntityFirestore:
		return xfirestore.NewLoader(spec, instanceId, s.config.Firestore.Client, spec.Namespace)

	case model.EntityVoid:
		return void.NewLoader(spec)

	case model.EntityAdmin:
		return s.adminLoader, nil

	default:
		return nil, fmt.Errorf("could not create loader, sink type '%s' not implemented, spec: %+v", spec.Sink.Type, spec)
	}
}

//
// Kafka config functions
//

func (s *StreamEntityFactory) createKafkaExtractorConfig(spec *model.Spec) *xkafka.Config {

	c := xkafka.NewExtractorConfig(
		spec,
		s.topicNamesFromSpec(spec.Source.Config.Topics),
		&kafkaTopicCreationMutex)

	// Deployment defaults
	props := xkafka.ConfigMap{
		"bootstrap.servers":        s.config.Kafka.DefaultBootstrapServers(spec.Source.Config.Provider),
		"enable.auto.commit":       true,
		"enable.auto.offset.store": false,
		"max.poll.interval.ms":     600000, // increase from 5 min default to 10 min

		// Maximum number of kilobytes per topic+partition in the local consumer queue.
		// To not go OOM if big backlog, set this low. Default is 1 048 576 KB  = 1GB per partition!
		// A few MBs seems to give good enough throughput while keeping memory requirements low.
		"queued.max.messages.kbytes": s.config.Kafka.QueuedMaxMessagesKb,

		// Possibly add fetch.message.max.bytes as well. Default is 1 048 576.
		// Initial maximum number of bytes per topic+partition to request when fetching messages from the broker.
		// "fetch.message.max.bytes": 1048576,
	}

	if spec.Source.Config.Provider == kafkaProviderConfluent {
		props["bootstrap.servers"] = s.config.Kafka.ConfluentBootstrapServer
		props["security.protocol"] = "SASL_SSL"
		props["sasl.mechanisms"] = "PLAIN"
		props["sasl.username"] = s.config.Kafka.ConfluentApiKey
		props["sasl.password"] = s.config.Kafka.ConfluentApiSecret
	}

	// Add all props from GEIST spec (could override deployment defaults)
	for _, prop := range spec.Source.Config.Properties {
		props[prop.Key] = prop.Value
	}

	c.SetProps(props)

	if spec.Source.Config.PollTimeoutMs != nil {
		c.SetPollTimout(*spec.Source.Config.PollTimeoutMs)
	} else {
		c.SetPollTimout(s.config.Kafka.PollTimeoutMs)
	}
	return c
}

func (s *StreamEntityFactory) createKafkaLoaderConfig(spec *model.Spec) *xkafka.Config {

	var sync bool
	if spec.Sink.Config.Synchronous != nil {
		sync = *spec.Sink.Config.Synchronous
	}

	c := xkafka.NewLoaderConfig(
		spec,
		s.topicSpecFromSpec(spec.Sink.Config.Topic),
		&kafkaTopicCreationMutex,
		sync)

	// Deployment defaults
	props := xkafka.ConfigMap{
		"bootstrap.servers":                     s.config.Kafka.DefaultBootstrapServers(spec.Sink.Config.Provider),
		"enable.idempotence":                    true,
		"acks":                                  "all",
		"max.in.flight.requests.per.connection": 5,
		"compression.type":                      "lz4",
		"auto.offset.reset":                     "earliest",
	}

	if spec.Sink.Config.Provider == kafkaProviderConfluent {
		props["bootstrap.servers"] = s.config.Kafka.ConfluentBootstrapServer
		props["security.protocol"] = "SASL_SSL"
		props["sasl.mechanisms"] = "PLAIN"
		props["sasl.username"] = s.config.Kafka.ConfluentApiKey
		props["sasl.password"] = s.config.Kafka.ConfluentApiSecret
	}

	// Add all props from GEIST spec (could override deployment defaults)
	for _, prop := range spec.Sink.Config.Properties {
		props[prop.Key] = prop.Value
	}

	c.SetProps(props)
	return c
}

//
// Pubsub config functions
//

func (s *StreamEntityFactory) createPubsubExtractorConfig(spec *model.Spec) *xpubsub.ExtractorConfig {
	return xpubsub.NewExtractorConfig(
		s.config.Pubsub.Client,
		spec,
		s.topicNamesFromSpec(spec.Source.Config.Topics),
		s.configureReceiveSettings(spec.Source.Config))
}

func (s *StreamEntityFactory) configureReceiveSettings(c model.SourceConfig) xpubsub.ReceiveSettings {
	var rs xpubsub.ReceiveSettings
	if c.MaxOutstandingMessages == nil {
		rs.MaxOutstandingMessages = s.config.Pubsub.MaxOutstandingMessages
	} else {
		rs.MaxOutstandingMessages = *c.MaxOutstandingMessages
	}

	if c.MaxOutstandingBytes == nil {
		rs.MaxOutstandingBytes = s.config.Pubsub.MaxOutstandingBytes
	} else {
		rs.MaxOutstandingBytes = *c.MaxOutstandingBytes
	}

	if c.Synchronous == nil {
		rs.Synchronous = false // no need to have this as deployment config
	} else {
		rs.Synchronous = *c.Synchronous
	}

	if c.NumGoroutines == nil {
		rs.NumGoroutines = 1 // no need to have this as deployment config
	} else {
		rs.NumGoroutines = *c.NumGoroutines
	}
	return rs
}

//
// Common config functions
//

func (s *StreamEntityFactory) topicNamesFromSpec(topicsInSpec []model.Topics) []string {
	var topicNames []string
	for _, topics := range topicsInSpec {
		if topics.Env == model.EnvironmentAll {
			topicNames = topics.Names
			break
		}
		if topics.Env == s.config.Env {
			topicNames = topics.Names
		}
	}
	return topicNames
}

func (s *StreamEntityFactory) topicSpecFromSpec(topicsInSpec []model.SinkTopic) *model.TopicSpecification {
	var topicSpec *model.TopicSpecification
	for _, topic := range topicsInSpec {
		if topic.Env == model.EnvironmentAll {
			topicSpec = topic.TopicSpec
			break
		}
		if topic.Env == s.config.Env {
			topicSpec = topic.TopicSpec
		}
	}
	if topicSpec != nil {
		if topicSpec.NumPartitions == 0 {
			topicSpec.NumPartitions = 1
		}
		if topicSpec.ReplicationFactor == 0 {
			topicSpec.ReplicationFactor = 1
		}
	}
	return topicSpec
}
