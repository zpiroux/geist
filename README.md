# Generic Event Ingestion and Stream Transloading (GEIST)

Geist provides cost-efficient high-performance capabilities to develop generic (or specific) services executing an arbitrary number of event streams between various sources and sinks, with a set of built-in single message transforms. It's an alternative to other more heavy-weight, constrained and/or costly products, although with a more slim feature scope.

Use cases include consuming events from Kafka or Pubsub and store them, or chosen parts of them, into BigTable, Firestore, BigQuery, or any other custom sink, with a schema as specified in a dynamically registered stream spec via Geist API. 

It also provides Geist API as a _Source_ with which it is possible for external services to publish events to a registered Geist stream, which then transforms and stores them into a chosen _Sink_.

Geist can concurrently execute an arbitrary amount of _different_ registered stream specs, in the same instance/pod. E.g. one stream with Kafka → BigTable and another with Pubsub → BigQuery, etc.

Although single event transform capabilities are provided, Geist is not meant to support complex stream processing, including stream joins and aggregations. That is better handled in products such as Spark, Kafka Streams, Dataflow/Beam, Flink, etc.

Its main purpose is to efficiently load streaming data, with optional transformations and enrichment, into a sink, where the analytical processing is done with other more appropriate products.

The Geist Go package is completely generic, only comprising the core engine and its domain model.
Geist Source and Sink connectors are found in separate repos. Custom connectors can also be provided dynamically by the service using Geist, registering them via Geist API for immediate use in stream specs.

## Quick Start
Prerequisites: Go 1.18

Install with:
```sh
go get -u github.com/zpiroux/geist
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

	geist.Run(ctx)
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
	gbigtable "github.com/zpiroux/geist-connector-gcp/bigtable"
	gkafka "github.com/zpiroux/geist-connector-kafka"
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

For a complete working example, see the [Emitter Stream](test/example/emitterstream/main.go).

### Validated source connectors
* [Kafka (vanilla and Confluent)](https://github.com/zpiroux/geist-connector-kafka)
* [Pubsub (GCP)](https://github.com/zpiroux/geist-connector-gcp)
* Geist API (natively supported)

### Validated sink connectors
* [Kafka (vanilla and Confluent)](https://github.com/zpiroux/geist-connector-kafka)
* [Firestore (GCP)](https://github.com/zpiroux/geist-connector-gcp)
* [BigTable (GCP)](https://github.com/zpiroux/geist-connector-gcp)
* [BigQuery (GCP)](https://github.com/zpiroux/geist-connector-gcp)

## Enrichment and custom stream logic
The native transformation entity provides a set of common transformations.
But to provide complete flexibility, adding capabilities for the Geist user to add custom logic such as enrichment of events, deduplication, complex filtering (if the native transform options are insufficient), etc, a client-managed hook function can be set in `geist.Config` prior to calling `geist.New()`.

The function assigned to `geist.Config.Hooks.PreTransformHookFunc` will be called for each event retrieved by the _Extractor_. A pointer to the raw event is provided to the func, which could modify its contents before Geist continues with downstream transformations and Sink processing.

The following example shows a hook func injecting a new field `"myNewField"` in the event JSON:

```go
c := geist.NewConfig()
c.Hooks.PreTransformHookFunc = MyEnricher
geist, err := geist.New(ctx, c)
...
func MyEnricher(ctx context.Context, event *[]byte) entity.HookAction {
    *event, err = geist.EnrichEvent(*event, "myNewField", "coolValue")
    return entity.HookActionProceed
}
```
Continued processing of each event can be controlled via the returned action value:
```go
HookActionProceed          // continue processing of this event
HookActionSkip             // skip processing of this event and take next
HookActionUnretryableError // let Geist handle this event as an unretryable error
HookActionShutdown         // shut down this stream instance
````


## Stream Categories
The quick start example showed one category of streams (interactive) where the Geist client can publish events explicitly. 

The other category is when a Geist client sets up an autonomous stream between a source and sink.
### Autonomous streams
Source set to `kafka` or `pubsub`.

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
When a Geist host pod is started, either from a new deployment or pod scaling, all registered specs are fetched from the native sink storage and booted up.
The default native spec is found in [regspec.go](internal/pkg/admin/regspec.go).

Any cross-pod synchronization, e.g. notification of newly updated stream specs, is done with an internal admin stream. It is managed and customizable in similar fashion as the Reg Spec and found in [adminspec.go](internal/pkg/admin/adminspec.go).

## Stream Spec Format
A stream spec has the following main fields/parts (additional fields are described further below):

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

For full spec structure with all optional and additional fields, see [specmodel.go](internal/pkg/admin/specmodel.go).

The Stream Spec struct is exposed as `geist.StreamSpec` into which a json bytes spec can be unmarshalled if needed.

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
            "provider": "confluent",
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
* All the events stored in a stream. Note that this is only meant for small datasets (supported by Firestore and BigTable connectors).
* Single event key-value lookup, retrieving the event stored in the sink with a given key/ID, provided it was stored in key/value mode (supported by Firestore and BigTable connectors).

Although functional, data retrieval is currently only used internally and not yet exposed by a Geist wrapper function.

## Performance Characteristics and Tuning
Geist has several stream config parameters exposed via the Stream Spec, to enable high-performance operation and stream throughput.

The main one governing concurrency is `Ops.StreamsPerPod`. This defines the amount of internal stream executors that will execute the full stream concurrently, for that particular stream.

As an example, say we have Kafka as a source, and a topic with 32 partitions. To maximize throughput (and/or minimize latency) the total number of stream executors should be 32 as well (or in practice less, since it's usually enough having each stream instance consuming from several partitions each). If the service using Geist is meant to run with two pods, the `Ops.StreamsPerPod` should be set to 16. There are no significant disadvantages with having the total amount of stream executors higher than the number of partitions, so having pods auto-scaled (creating more than two pods) will not be an issue.

Since each stream executor runs as a light-weight Goroutine, thousands of stream executors can easily be handled by a single pod, obviously provided pod resources are meant for the particular type of stream processing logic.

But it's usually a good approach to start with a low number of streams per pod, and increase based on test results.

Further parameters can be found in the stream spec definition ([specmodel.go](internal/pkg/admin/specmodel.go)), with general ones in the `Ops` struct and Source/Sink specific ones in respective `SourceConfig` and `SinkConfig` struct.

## Limitations and improvement areas
Although Geist has been run in production with heavy load, no data-loss, and zero downtime for ~two years, it makes no guarantees that all combinations of stream spec options will work fully in all cases.

The following types of streams have been run extensively and concurrently with high throughput:

* Kafka (Confluent) → BigTable
* Kafka (Confluent) → BigQuery

Additionally, Pubsub as source and Firestore as sink have been used extensively but with limited traffic.

### Transforms
The current native transform package provides features as required by a set of use cases. In contrast to source/sink connectors it's not yet possible to add custom ones. However, any custom transformation logic can still be injected via the PreTransformHook functionality.

Chaining of transforms could be improved.

Although functional, regex transform options have some remaining todos.

Geist is not meant to support complex stream processing, including stream joins and aggregations. That is better handled in products such as Spark, Kafka Streams, Dataflow/Beam, Flink, etc.

### Event schema
Only JSON currently supported.

### Logging configurability
Internal logging could be made more configurable externally.

Log level is based on environment variable LOG_LEVEL. If not set, INFO will be used.

### Spec schema
For historical reasons some fields in the [Spec schema](internal/pkg/admin/specmodel.go) are only used for specific source/sink types, even though the customConfig enables any arbitrary config to be used.

## Contact
info @ zpiroux . com

## License
Geist source code is available under the MIT License.