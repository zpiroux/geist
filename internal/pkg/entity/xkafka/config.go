package xkafka

import (
	"fmt"
	"sync"

	"github.com/zpiroux/geist/internal/pkg/model"
)

type ConfigMap map[string]any

type Config struct {
	spec               *model.Spec
	topics             []string                  // list of topics to consume from by Extractor
	sinkTopic          *model.TopicSpecification // topic to (create and) publish to by Loader
	pollTimeoutMs      int                       // timeoutMs in Consumer Poll function
	configMap          ConfigMap                 // supports all possible Kafka consumer properties
	topicCreationMutex *sync.Mutex
	synchronous        bool
}

func (c *Config) String() string {
	return fmt.Sprintf("topics: %v, pollTimeoutMs: %d, synchronous: %v, props: %+v",
		c.topics, c.pollTimeoutMs, c.synchronous, displayConfig(c.configMap))
}

func NewExtractorConfig(
	spec *model.Spec,
	topics []string,
	topicCreationMutex *sync.Mutex) *Config {
	return &Config{
		spec:               spec,
		topics:             topics,
		configMap:          make(ConfigMap),
		topicCreationMutex: topicCreationMutex,
	}
}

func NewLoaderConfig(
	spec *model.Spec,
	topic *model.TopicSpecification,
	topicCreationMutex *sync.Mutex,
	sync bool) *Config {
	return &Config{
		spec:               spec,
		sinkTopic:          topic,
		configMap:          make(ConfigMap),
		topicCreationMutex: topicCreationMutex,
		synchronous:        sync,
	}
}

func (c *Config) SetPollTimout(timeout int) {
	c.pollTimeoutMs = timeout
}

func (c *Config) SetKafkaProperty(prop string, value any) {
	c.configMap[prop] = value
}

func (c *Config) SetProps(props ConfigMap) {
	for k, v := range props {
		c.configMap[k] = v
	}
}

func displayConfig(in ConfigMap) ConfigMap {
	out := make(ConfigMap)
	for k, v := range in {
		if k != "sasl.password" {
			out[k] = v
		}
	}
	return out
}
