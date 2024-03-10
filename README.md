# Generic Event Ingestion and Stream Transloading (GEIST)
<div>

[![Go Report Card](https://goreportcard.com/badge/github.com/zpiroux/geist)](https://goreportcard.com/report/github.com/zpiroux/geist)
[![Go Reference](https://pkg.go.dev/badge/github.com/zpiroux/geist.svg)](https://pkg.go.dev/github.com/zpiroux/geist)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=zpiroux_geist&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=zpiroux_geist)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=zpiroux_geist&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=zpiroux_geist)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=zpiroux_geist&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=zpiroux_geist)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=zpiroux_geist&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=zpiroux_geist)

</div>

Geist provides cost-efficient high-performance capabilities to develop generic (or specific) services executing an arbitrary number of event streams between various sources and sinks, with a set of built-in single message transforms. It's an alternative to other more heavy-weight, constrained and/or costly products, although with a more slim feature scope.

Use cases include consuming events from Kafka or Pubsub and store them, or chosen parts of them, into BigTable, Firestore, BigQuery, or any other custom sink, with a schema as specified in a dynamically registered stream spec via Geist API. 

It also provides Geist API as a _Source_ with which it is possible for external services to publish events to a registered Geist stream, which then transforms and stores them into a chosen _Sink_.

Geist can concurrently execute an arbitrary amount of _different_ registered stream specs, in the same instance/pod. E.g. one stream with Kafka → BigTable and another with Pubsub → BigQuery, etc.

Although single event transform capabilities are provided, Geist is not meant to support complex stream processing, including stream joins and aggregations. That is better handled in products such as Spark, Kafka Streams, Dataflow/Beam, Flink, etc.

Its main purpose is to efficiently load streaming data, with optional transformations and enrichment, into a sink, where the analytical processing is done with other more appropriate products.

The Geist Go package is completely generic, only comprising the core engine and its domain model.
Geist Source and Sink connectors are found in separate repos. Custom connectors can also be provided dynamically by the service using Geist, registering them via Geist API for immediate use in stream specs.

## Quick Start
Prerequisites: Go 1.19

Install with:
```sh
go get github.com/zpiroux/geist
```

Example of simplified Geist test usage (error handling omitted) with an interactive stream where the sink simply logs info on transformed event data (using the built-in `void` debug/test sink).
```go
func main() {
	ctx := context.Background()
	g, err := geist.New(ctx, geist.NewConfig())

	go func() {
		streamId, err := g.RegisterStream(ctx, spec)
		resId, err := g.Publish(ctx, streamId, []byte("Hi there!"))
		g.Shutdown(ctx)
	}()

	g.Run(ctx)
}

var spec = []byte(`
    {
        "namespace": "my",
        "streamIdSuffix": "tiny-stream",
        "description": "Tiny test stream logging event data to console.",
        "version": 1,
        "source": {
            "type": "geistapi"
        },
        "transform": {
            "extractFields": [
                {
                    "fields": [
                        {
                            "id": "rawEvent"
                        }
                    ]
                }
            ]
        },
        "sink": {
            "type": "void",
            "config": {
                "properties": [
                    {
                        "key": "logEventData",
                        "value": "true"
                    }
                ]
            }
        }
    }
`)
```


## Using Connectors
The Quick Start example is a minimal integration. To do something more useful, external Source/Sink connectors need to be registered. As an example, say we want to deploy a stream consuming events from Kafka and persist them in BigTable. For this we need to register a Kafka Source Extractor and a BigTable Sink Loader.

Install with:
```sh
go get github.com/zpiroux/geist-connector-kafka
go get github.com/zpiroux/geist-connector-gcp
```
Register connectors prior to starting up Geist with (error handling omitted):
```go
import (
	"github.com/zpiroux/geist"
	"github.com/zpiroux/geist-connector-gcp/gbigtable"
	"github.com/zpiroux/geist-connector-kafka/gkafka"
)

...
geistConfig := geist.NewConfig()

kafkaConfig := &gkafka.Config{ /* add config */ }
btConfig := gbigtable.Config{ /* add config */ }

ef := gkafka.NewExtractorFactory(kafkaConfig)
lf, err := gbigtable.NewLoaderFactory(ctx, btConfig)

err = geistConfig.RegisterExtractorType(ef)
err = geistConfig.RegisterLoaderType(lf)

g, err := geist.New(ctx, geistConfig)
...
```
The service using Geist could in this way also register its own custom connectors, provided they adhere to the connector interfaces, etc.

For a complete working example of providing a custom connector, see the [Emitter Stream](test/example/emitterstream/main.go).

### Validated source connectors
* [Kafka](https://github.com/zpiroux/geist-connector-kafka)
* [Pubsub (GCP)](https://github.com/zpiroux/geist-connector-gcp)
* Geist API (natively supported)

### Validated sink connectors
* [Kafka](https://github.com/zpiroux/geist-connector-kafka)
* [BigQuery (GCP)](https://github.com/zpiroux/geist-connector-bigquery)
* [Firestore (GCP)](https://github.com/zpiroux/geist-connector-gcp)
* [BigTable (GCP)](https://github.com/zpiroux/geist-connector-gcp)

## Enrichment and custom stream logic
The native transformation entity provides a set of common transformations.
But to provide complete flexibility, adding capabilities for the Geist user to add custom logic such as enrichment of events, deduplication, complex filtering (if the native transform options are insufficient), etc., client-managed hook functions can be set in `geist.Config` prior to calling `geist.New()`.

There are two different hook func types, `PreTransformHookFunc` and `PostTransformHookFunc`, which are called before and after event transformations.

The function assigned to `geist.Config.Hooks.PreTransformHookFunc` will be called for each event retrieved by the _Extractor_. A pointer to the raw event is provided to the func, which could modify its contents before Geist continues with downstream transformations and Sink processing.

The function assigned to `geist.Config.Hooks.PostTransformHookFunc` will be called similarly, but after transformations and providing the transformed field maps instead of the raw incoming event.

The following example shows hook funcs injecting two new fields in the event, as eventually received by the sink.

```go
c := geist.NewConfig()
c.Hooks.PreTransformHookFunc = MyPreTransformEnricher
c.Hooks.PostTransformHookFunc = MyPostTransformEnricher
g, err := geist.New(ctx, c)
...

func MyPreTransformEnricher(ctx context.Context, spec *entity.Spec, event *[]byte) entity.HookAction {
    *event, err = geist.EnrichEvent(*event, "myNewField1", "someValue") 
    // geist.EnrichEvent() is just a convenience function, used here for example purposes
    return entity.HookActionProceed
}

func MyPostTransformEnricher(ctx context.Context, spec *entity.Spec, event *[]*entity.Transformed) entity.HookAction {
    (*event)[0].Data["myNewField2"] = "someOtherValue"
    return entity.HookActionProceed
}

```
Continued processing of each event can be controlled via the returned action value:
```go
HookActionProceed          // continue processing of this event
HookActionSkip             // skip processing of this event and take next
HookActionRetryableError   // let Geist handle this event as a retryable error
HookActionUnretryableError // let Geist handle this event as an unretryable error
HookActionShutdown         // shut down this stream instance
````


## Stream Categories
The quick start example showed one category of streams (interactive) where the Geist client can publish events explicitly. 

The other category is when a Geist client sets up an autonomous stream between a source and sink.
### Autonomous streams
Source set to `kafka`, `pubsub` or similar stream data sources.

The host service sets up a fully automated stream between a source and sink, with `geist.RegisterStream()`, e.g. consuming from Kafka and inserting into BigTable, and only manage start and stop of the stream.

### Interactive streams
Source set to `geistapi`.

The host service has its own API or logic on top and explicitly publish events to the Geist stream with `geist.Publish()`, for further transformation and insertion into the chosen sink, as specified in the stream spec when registering the stream with `geist.RegisterStream()`. One example could be a thin REST API service using Geist to publish events to Kafka or any other sink.

## Service modes of operation
There are two main types of service use-cases or deployment setup types for the hosting service using Geist.
### Service with specific use-case with self-contained stream specs
This is the simplest mode and useful for small dedicated services.

Example use case: A service receiving (or producing) certain type of events that needs to be published to Kafka and/or stored in BigTable (etc.).

In this mode Geist merely works as a generic adaptor for the sinks.
The stream specs to be used could be built-in in the hosting service and registered during startup.

### Generic stream platform service with stream spec persistence
This is the mode for which Geist was developed in the first place, and is meant to run as a Kubernetes deployment.

In this mode Geist functions as a generic run-time and stream management service, with an external wrapper API, e.g. REST API, provided by the hosting service, with which other services dynamically could register and deploy their own streams without any downtime or re-deployment of Geist or its hosting service.

Registered specs are persisted by Geist with an internally defined (but customizable) Stream Spec.
When a Geist host pod is started, either from a new deployment or pod scaling, all registered specs are fetched from the specified sink storage and booted up.
The default native spec is found in [regspec.go](internal/pkg/admin/regspec.go), but any custom storage type can be used (as specified in `geist.Config.Registry`), as long as it has a matching Geist connector.

Any cross-pod synchronization, e.g. notification of newly updated stream specs, is done with an internal admin stream. It is managed and customizable in similar fashion as the Reg Spec. The default native spec is found in [adminspec.go](internal/pkg/admin/adminspec.go), but any other custom source types can be used here as well, as specified in `geist.Config.AdminStream`.

## Stream Spec Format
A stream spec has the following required main fields/parts (additional fields are described further below):

```
{
  "namespace": "<service or feature acting as stream owner>",
  "streamIdSuffix": "<stream subtype>",
  "version": <stream spec version, of int type>,
  "description": "<description of the spec>",
  "source": {
    <source spec>
  },
  "transform": {
    <transform spec>
  },
  "sink": {
    <sink spec>
  }
}
```
The ID of the new stream returned from `geist.RegisterStream()` in a successful registration is constructed as:

`<"namespace">-<"streamIdSuffix">`

For full spec structure with all optional and additional fields, see [spec.go](entity/spec.go).

During spec registration the JSON spec is converted to an `entity.Spec` struct for internal usage. If needed, this can also be done externally by using `entity.NewSpec(specBytes)`, which ensures schema validation and proper default values.

### Operational Config

For a more detailed operational control and tuning of each stream the optional `"ops"` config can be used.

Full default config used, if not specified in the stream spec, is the following:

```json
  "ops": {
    "streamsPerPod": 1,
    "microBatch": false,
    "microBatchSize": 500,
    "microBatchBytes": 5000000,
    "microBatchTimeoutMs": 15000,
    "maxEventProcessingRetries": 5,
    "maxStreamRetryBackoffIntervalSec": 300,
    "handlingOfUnretryableEvents": "default",
    "logEventData": false
  }
```

See the `Ops` struct in [spec.go](entity/spec.go) for full documentation.

For scenarios where the streams need to be deployed to multiple environments with different surrounding context, an additional environment specific config option is available using the `opsPerEnv` field. With this field, the above Ops config can look different for each named environment.

Say we have two different production environments, `prod-x` and `prod-y`, we can then specify different config per environment using the following format (example):

```json
  "opsPerEnv": {
    "prod-x": {
      "streamsPerPod": 8
    },
    "prod-y": {
      "streamsPerPod": 16
    }
  }
```

The environment string to match with the one provided in the spec is set in `geist.Config.Registry.Env` when creating Geist with `geist.New(ctx, config)`.

If `opsPerEnv` is provided and a match is found for the deployed environment type, the specified `opsPerEnv` config will be set as the main `"ops"` specification to use in this specific deployment.


### Example spec
The simple spec below exemplifies an autonomous stream using Kafka as source and BigTable as sink, inserting raw events in a table with a single column named `event`, row-key as `<fooId>#<barType>`, and a TTL of 31 days.

```json
{
    "namespace": "my",
    "streamIdSuffix": "kafkabigtable-stream",
    "description": "Example of a simple stream spec Kafka --> BigTable",
    "version": 1,
    "ops": {
        "streamsPerPod": 2
    },
    "source": {
        "type": "kafka",
        "config": {
            "topics": [
                {
                    "env": "all",
                    "names": [
                        "foo.events"
                    ]
                }
            ],
            "properties": [
                {
                    "key": "group.id",
                    "value": "my-kafkabigtable-stream"
                }
            ]
        }
    },
    "transform": {
        "extractFields": [
            {
                "fields": [
                    {
                        "id": "someField",
                        "jsonPath": "data.fooId"
                    },
                    {
                        "id": "someOtherField",
                        "jsonPath": "data.barType"
                    },
                    {
                        "id": "rawEvent"
                    }
                ]
            }
        ]
    },
    "sink": {
        "type": "bigtable",
        "config": {
            "customConfig": {
                "tables": [
                    {
                        "name": "foo_events",
                        "rowKey": {
                            "keys": [
                                "someField",
                                "someOtherField"
                            ],
                            "delimiter": "#"
                        },
                        "columnFamilies": [
                            {
                                "name": "d",
                                "garbageCollectionPolicy": {
                                    "type": "maxAge",
                                    "value": 744
                                },
                                "columnQualifiers": [
                                    {
                                        "id": "rawEvent",
                                        "name": "event"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        }
    }
}
```

More example specs, with more sink, source and transform types, can be found in [test/specs](test/specs).


## Updating or disabling a stream
A running stream can be updated dynamically with a new call to `geist.RegisterStream()` provided the spec's mandatory version number field is incremented. The provided modified spec will fully replace the existing one. This will seamlessly cause an internal restart of the stream's executors using the updated spec.

To disable a spec it needs to have the root field `"disabled"` added and set to `true`. This will shut down all the stream's executors directly.
To enable the stream again, do a new update of spec (increase version number) and set `"disabled"` to `false`.

## Getting information about/from streams
The following data can be retrieved:
* The full list of specs for all registered streams.
* The full list of stream IDs together with status on enabled/disabled.
* The spec for a single stream from a stream ID.

The following event retrieval options are not yet exposed by a Geist API wrapper function, but used internally to enable spec persistence with arbitrary sink types.
* All the events stored in a stream. Note that this is only meant for small datasets (example support: Firestore and BigTable connectors).
* Single event key-value lookup, retrieving the event stored in the sink with a given key/ID, provided it was stored in key/value mode (example support: Firestore and BigTable connectors).

## Performance Characteristics and Tuning
Geist has several stream config parameters exposed via the Stream Spec, to enable high-performance operation and stream throughput.

The main one governing concurrency is `Ops.StreamsPerPod`. This defines the amount of internal stream executors that will execute the full stream concurrently, for that particular stream.

As an example, say we have Kafka as a source, and a topic with 32 partitions. To maximize throughput (and/or minimize latency) the total number of stream executors should be 32 as well (or in practice less, since it's usually enough having each stream instance consuming from several partitions each). If the service using Geist is meant to run with two pods, the `Ops.StreamsPerPod` should be set to 16. There are no significant disadvantages with having the total amount of stream executors higher than the number of partitions, so having pods auto-scaled (creating more than two pods) will not be an issue.

Since each stream executor runs as a light-weight Goroutine, thousands of stream executors can easily be handled by a single pod, obviously provided pod resources are meant for the particular type of stream processing logic.

But it's usually a good approach to start with a low number of streams per pod, and increase based on test results.

Further parameters can be found in the stream spec definition ([spec.go](entity/spec.go)), with general ones in the `Ops` struct and Source/Sink specific ones in respective `SourceConfig` and `SinkConfig` struct.

## Observability
### Logging and Notification Events
Geist sends important and informational log events to a notification channel accessible from `geist.NotifyChannel()`. The size of the chan buffer can be specified during Geist creation with `Config.Ops.NotifyChanSize`.

If usage of the notification channel is not needed, Geist can also be configured to perform standard logging of those events on its native format. This can be enabled by setting `Config.Ops.Log` to true.

### Metrics
Real-time event processing metrics can be retrieved with `geist.Metrics()`. The following metrics are provided per stream ID:
```go
type Metrics struct {

	// Total number of events sent to Executor's ProcessEvent() by the Extractor,
	// regardless of the outcome of downstream processing.
	EventsProcessed int64

	// Total time spent by Executor processing all extracted events
	EventProcessingTimeMicros int64

	// Total number of event batches sent from Extractor to Sink loader via Executor
	Microbatches int64

	// Total amount of event data processed (as sent from Extractor)
	BytesProcessed int64

	// Total number of events successfully processed by the sink.
	EventsStoredInSink int64

	// Total time spent ingesting transformed events in the sink successfully
	SinkProcessingTimeMicros int64

	// Total number of successfull calls to the Sink's StreamLoad method
	SinkOperations int64

	// Total amount of data successfully ingested
	BytesIngested int64
}
```

## Event Simulation
For various test purposes the native source type `"eventsim"` can be used as source in a stream spec. Using this as source in a deployed stream will continuously send generated events to the sink, with an event schema and randomization as specified in the stream spec.

For full config and spec field documentation, see [eventsim.go](internal/pkg/entity/eventsim/eventsim.go).

For a stream spec example with most options included, see [eventsim_test.go](internal/pkg/entity/eventsim/eventsim_test.go).

The following small example spec creates a stream which continuously (every 3 seconds) sends a variably sized microbatch of berry picking events to the sink. If the sink was a database a visualized timeseries event count would have the form of a sine wave.

Stream spec (only showing the source section):

```json
{
    "...": "",
    "source": {
        "type": "eventsim",
        "config": {
            "customConfig": {
                "simResolutionMilliseconds": 3000,
                "eventGeneration": {
                    "type": "sinusoid",
                    "minCount": 1,
                    "maxCount": 50,
                    "periodSeconds": 86400,
                    "peakTime": "2023-03-25T11:00:00Z"
                },
                "eventSpec": {
                    "fields": [
                        {
                            "field": "eventId",
                            "randomizedValue": {
                                "type": "uuid"
                            }
                        },
                        {
                            "field": "dateReported",
                            "randomizedValue": {
                                "type": "isoTimestampMilliseconds",
                                "jitterMicroseconds": 6000000
                            }
                        },
                        {
                            "field": "berriesPicked.type",
                            "predefinedValues": [
                                {
                                    "value": "blueberry",
                                    "frequencyFactor": 60
                                },
                                {
                                    "value": "blackberry",
                                    "frequencyFactor": 30
                                },
                                {
                                    "value": "cloudberry",
                                    "frequencyFactor": 10
                                }
                            ]
                        },
                        {
                            "field": "berriesPicked.amount",
                            "randomizedValue": {
                                "type": "int",
                                "min": 0,
                                "max": 75
                            }
                        }
                    ]
                }
            }
        }
    },
    "transform": {},
    "sink": {}
}
```


Example of generated event:
```json
{
    "eventId": "2446c459-0867-4312-9b0e-5b645728b9de",
    "dateReported": "2023-04-12T21:33:44.707Z",
    "berriesPicked": {
        "type": "cloudberry",
        "amount": 36
    }
}
``` 

## Limitations and improvement areas
Although Geist has been run in production with heavy load, no data-loss, and zero downtime for three+ years, it makes no guarantees that all combinations of stream spec options will work fully in all cases.

The following types of streams have been run extensively and concurrently with high throughput:

* Kafka → BigTable
* Kafka → BigQuery
* Kafka → Various custom-built connectors for products both in GCP and AWS (Redshift, S3, OpenSearch).

Additionally, Pubsub as source and Firestore as sink have been used extensively but with limited traffic.

### Transforms
The current native transform package provides features as required by a set of use cases. In contrast to source/sink connectors it's not yet possible to add custom ones. However, any custom transformation logic can still be injected via the PreTransformHook functionality.

Chaining of transforms could be improved.

Although functional, regex transform options have some remaining todos.

Geist is not meant to support complex stream processing, including stream joins and aggregations. That is better handled in products such as Spark, Kafka Streams, Dataflow/Beam, Flink, etc.

### Event schema
Only JSON currently supported.

### Spec schema
For historical reasons some fields in the [Spec schema](entity/spec.go) are only used for specific source/sink types, even though the `customConfig` enables any arbitrary config to be used.

## Contact
info @ zpiroux . com

## License
Geist source code is available under the MIT License.