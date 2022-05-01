# Generic Event Ingestion and Stream Transloading (GEIST)
Geist provides cost-efficient high-performance capabilities to develop generic (or specific) services executing an arbitrary number of event streams between various sources and sinks, with a set of built-in single message transforms. It's an alternative to other more heavy-weight, constrained and/or costly products, although with a more slim feature scope.

Use cases include consuming events from Kafka or Pubsub and store them, or chosen parts of them, into BigTable, Firestore or BigQuery, with a schema as specified in a dynamically registered stream spec via Geist API. 

It also provides Geist API as a _Source_ with which it is possible for external services to publish events to a registered Geist stream, which then transforms and stores them into a chosen _Sink_.

Geist can concurrently execute an arbitrary amount of _different_ registered stream specs, in the same instance/pod. E.g. one stream with Kafka → BigTable and another with Pubsub → BigQuery, etc.

Although single event transform capabilities are provided, Geist is not meant to support complex stream processing, including stream joins and aggregations. That is better handled in products such as Spark, Kafka Streams, Dataflow/Beam, Flink, etc.

Its main purpose is to efficiently load streaming data into a sink, where the analytical processing and enrichment is done with other more appropriate products.

## Quick Start
Prerequisites: Go 1.18

Install with:
```sh
go get -u github.com/zpiroux/geist
```

Example of simplified Geist test usage (error handling omitted) with an interactive stream where the sink outputs event data to console.
```go
func main() {
	ctx := context.Background()
	geist, err := geist.New(ctx, geist.Config{})

	go func() {
		streamId, err := geist.RegisterStream(ctx, spec)
		resId, err := geist.Publish(ctx, streamId, []byte("Hi there!"))
		geist.Shutdown(ctx)
	}()

	geist.Run(ctx)
}

var spec = []byte(`{
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
The default/native sink in this spec is GCP Firestore. When a Geist host pod is started, either from a new deployment or pod scaling, all registered specs are fetched from the native sink storage and booted up.
The default native spec is found in [regspec.go](internal/pkg/model/regspec.go).

Any cross-pod synchronization, e.g. notification of newly updated stream specs, is done with an internal admin stream. It is managed in similar fashion as the Reg Spec and the default native one is using GCP Pubsub and found in [adminspec.go](internal/pkg/model/adminspec.go).

## Supported sources
* Kafka (vanilla and Confluent)
* Pubsub (GCP)
* Geist API

## Supported sinks
* Kafka (vanilla and Confluent)
* Firestore (GCP)
* BigTable (GCP)
* BigQuery (GCP)
* (Pubsub - todo)

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
The ID of the new stream returned from `geist.RegisterStream()` in a successful registration is always constructed as:

`<"namespace">-<"streamIdSuffix">`

For full spec structure with all optional and additional fields, see [specmodel.go](internal/pkg/model/specmodel.go).

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
* All the events stored in a stream. Note that this is only meant for small datasets, and for now only supported when using Firestore or BigTable as sink.
* Single event key-value lookup, retrieving the event stored in the sink with a given key/ID, provided it was stored in key/value mode. Only supported by Firestore and BigTable sink.

Although functional, data retrieval is currently only used internally and not yet exposed by a Geist wrapper function.

## Performance Characteristics and Tuning
Geist has several stream config parameters exposed via the Stream Spec, to enable high-performance operation and stream throughput.

The main one governing concurrency is `Ops.StreamsPerPod`. This defines the amount of internal stream executors that will execute the full stream concurrently, for that particular stream.

As an example, say we have Kafka as a source, and a topic with 32 partitions. To maximize throughput (and/or minimize latency) the total number of stream executors should be 32 as well (or in practice less, since it's usually enough having each stream instance consuming from several partitions each). If the service using Geist is meant to run with two pods, the `Ops.StreamsPerPod` should be set to 16. There are no significant disadvantages with having the total amount of stream executors higher than the number of partitions, so having pods auto-scaled (creating more than two pods) will not be an issue.

Since each stream executor runs as a light-weight Goroutine, thousands of stream executors can easily be handled by a single pod, obviously provided pod resources are meant for the particular type of stream processing logic.

But it's usually a good approach to start with a low number of streams per pod, and increase based on test results.

Further parameters can be found in the stream spec definition ([specmodel.go](internal/pkg/model/specmodel.go)), with general ones in the `Ops` struct and Source/Sink specific ones in respective `SourceConfig` and `SinkConfig` struct.

## Limitations and improvement areas
Although Geist has been run in production with heavy load, no data-loss, and zero downtime for ~two years, it makes no guarantees that all combinations of stream spec options will work fully in all cases.

The following types of streams have been run extensively and concurrently with high throughput:

* Kafka (Confluent) → BigTable
* Kafka (Confluent) → BigQuery

Additionally, Pubsub as source and Firestore as sink have been used extensively but with limited traffic.

### Transforms
The current transform package provides features as required by a set of use cases. Its implementation could easily be replaced or extended in code, but there is currently no mechanism to replace or extend transformation logic in a plug-in fashion without rebuilding.

Chaining of transforms could be improved.

Although functional, regex transform options have some remaining todos.

Geist is not meant to support complex stream processing, including stream joins and aggregations. That is better handled in products such as Spark, Kafka Streams, Dataflow/Beam, Flink, etc.

### Event schema
Only JSON currently supported.

### Kafka Sink entity
Although the Kafka _Source_ entity exhibits high performance and guaranteed delivery, the Kafka _Sink_ entity has lower throughput. 

There is an option in the Stream Spec to increase the sink throughput for a given stream, but that will disable guaranteed delivery, e.g. in case of Geist host crashing, so should only be used for non-critical streams.

To fix this, message batching in the sink should be added.

### Kafka providers
Secure access is enabled when using spec provider option `"confluent"`, where API key and secret are provided in `geist.Config.Kafka` when creating Geist with `geist.New()`.

If using vanilla Kafka with spec option `"native"` it only supports plain access without authentication.

### Pubsub extractor DLQ
The Kafka Extractor supports automated DLQ handling of unretryable events (e.g. corrupt events that can't be transformed or processed by the Sink), if that option (`"dlq"`) is chosen in the Stream Spec, but the Pubsub Extractor currently only supports the options `"discard"` and `"fail"`.

Using Pubsub's built-in dead-letter topic option in the subscription as a work-around until this feature is added in the Extractor will currently not have the intended effect.

### Logging configurability
Internal logging could be made more configurable externally.

Log level is based on environment variable LOG_LEVEL. If not set, INFO will be used.

## Contact
info @ zpiroux . com

## License
Geist source code is available under the MIT License.