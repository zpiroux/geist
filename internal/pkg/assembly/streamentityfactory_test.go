package assembly

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/internal/pkg/model"
)

type envType int

const (
	envDev envType = iota
	envStage
	envProd
)

var envs = map[envType]model.Environment{
	envDev:   model.EnvironmentDev,
	envStage: model.EnvironmentStage,
	envProd:  model.EnvironmentProd,
}

func TestTopicNamesFromSpec(t *testing.T) {

	cfg := Config{Env: envs[envDev]}
	sef := NewStreamEntityFactory(cfg)

	spec, err := model.NewSpec(kafkaToVoidStreamSplitEnv)
	assert.NoError(t, err)
	topics := sef.topicNamesFromSpec(spec.Source.Config.Topics)
	assert.Equal(t, topics, []string{"foo.events.dev"})

	sef.config.Env = envs[envStage]
	topics = sef.topicNamesFromSpec(spec.Source.Config.Topics)
	assert.Equal(t, topics, []string{"foo.events.stage"})

	sef.config.Env = envs[envProd]
	topics = sef.topicNamesFromSpec(spec.Source.Config.Topics)
	assert.Equal(t, topics, []string{"foo.events"})

	spec, err = model.NewSpec(kafkaToVoidStreamCommonEnv)
   assert.NoError(t, err)
	for _, env := range envs {
		sef.config.Env = env
		topics = sef.topicNamesFromSpec(spec.Source.Config.Topics)
		assert.Equal(t, topics, []string{"foo.events", "bar.events"})
	}

	// Test handling of missing envs
	spec, err = model.NewSpec(kafkaToKafkaDevOnly)
	assert.NoError(t, err)
	sef.config.Env = envs[envProd]
	topics = sef.topicNamesFromSpec(spec.Source.Config.Topics)
	assert.Empty(t, topics)

	topicSpec := sef.topicSpecFromSpec(spec.Sink.Config.Topic)
	assert.Nil(t, topicSpec)
}

var (
	kafkaToVoidStreamSplitEnv = []byte(`
{
   "namespace": "geisttest",
   "streamIdSuffix": "mock-1",
   "version": 1,
   "description": "...",
   "source": {
      "type": "kafka",
      "config": {
         "provider": "confluent",
         "topics": [
            {
               "env": "dev",
               "names": [
                  "foo.events.dev"
               ]
            },
            {
               "env": "stage",
               "names": [
                  "foo.events.stage"
               ]
            },
            {
               "env": "prod",
               "names": [
                  "foo.events"
               ]
            }
         ],
         "properties": [
            {
               "key": "group.id",
               "value": "geisttest-mock-1"
            }
         ]
      }
   },
   "transform": {},
   "sink": {
      "type": "void"
   }
}
`)
	kafkaToKafkaDevOnly = []byte(`
{
   "namespace": "geisttest",
   "streamIdSuffix": "mock-1",
   "version": 1,
   "description": "...",
   "source": {
      "type": "kafka",
      "config": {
         "provider": "confluent",
         "topics": [
            {
               "env": "dev",
               "names": [
                  "foo.events.dev"
               ]
            }
         ],
         "properties": [
            {
               "key": "group.id",
               "value": "geisttest-mock-1"
            }
         ]
      }
   },
   "transform": {
      "extractFields": [
         {
            "fields": [
               {
                  "id": "payload"
               }
            ]
         }
      ]
   },
   "sink": {
      "type": "kafka",
      "config": {
         "topic": [
            {
               "env": "dev",
               "topicSpec": {
                  "name": "geisttest.events.dev",
                  "numPartitions": 6,
                  "replicationFactor": 3
               }
            }
         ],
         "properties": [
            {
               "key": "client.id",
               "value": "geisttest_mock-1"
            }
         ],
         "message": {
            "payloadFromId": "payload"
         }
      }
   }
}
`)
	kafkaToVoidStreamCommonEnv = []byte(`
{
   "namespace": "geisttest",
   "streamIdSuffix": "mock-2",
   "version": 1,
   "description": "...",
   "source": {
      "type": "kafka",
      "config": {
         "provider": "confluent",
         "topics": [
            {
               "env": "all",
               "names": [
                  "foo.events",
                  "bar.events"
               ]
            }
         ],
         "properties": [
            {
               "key": "group.id",
               "value": "geisttest-mock-2"
            }
         ]
      }
   },
   "transform": {},
   "sink": {
      "type": "void"
   }
}
`)
)
