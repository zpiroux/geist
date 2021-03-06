package entity

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/xeipuuv/gojsonschema"
)

// General Ops defaults
const (
	DefaultStreamsPerPod                    = 1
	DefaultMicroBatchSize                   = 500
	DefaultMicroBatchBytes                  = 5000000
	DefaultMicroBatchTimeoutMs              = 15000
	DefaultMaxEventProcessingRetries        = 5
	DefaultMaxStreamRetryBackoffIntervalSec = 300
)

// Available options for Ops.HandlingOfUnretryableEvents
const (
	HoueDefault = "default"
	HoueDiscard = "discard"
	HoueDlq     = "dlq"
	HoueFail    = "fail"
)

// Data processing and ingestion options
const GeistIngestionTime = "@GeistIngestionTime"

// Spec implements the GEIST ETL Stream Spec interface and specifies how each ETL stream should
// be executed from Source to Transform to Sink. Specs are registered and updated through
// an ETL stream of its own, as specified by the built-in SpecRegistrationSpec.
// The Namespace + StreamIdSuffix combination must be unique (forming a GEIST ETL Stream ID).
// To succeed with an upgrade of an existing spec the version number needs to be incremented.
type Spec struct {
	Namespace      string    `json:"namespace"`
	StreamIdSuffix string    `json:"streamIdSuffix"`
	Description    string    `json:"description"`
	Version        int       `json:"version"`
	Disabled       bool      `json:"disabled"`
	Ops            Ops       `json:"ops"`
	Source         Source    `json:"source"`
	Transform      Transform `json:"transform"`
	Sink           Sink      `json:"sink"`
}

// NewSpec creates a new Spec from JSON and validates both against JSON schema and the transformation
// logic on the created spec.
func NewSpec(specData []byte) (*Spec, error) {
	var spec Spec
	if len(specData) == 0 {
		return nil, errors.New("no spec data provided")
	}

	if err := validateRawJson(specData); err != nil {
		return nil, err
	}

	spec.SetRequiredDefaults()
	err := json.Unmarshal(specData, &spec)
	if err == nil {
		err = spec.Validate()
	}
	return &spec, err
}

func NewEmptySpec() *Spec {
	var spec Spec
	spec.SetRequiredDefaults()
	return &spec
}

func (s *Spec) Id() string {
	return s.Namespace + "-" + s.StreamIdSuffix
}

func (s *Spec) IsDisabled() bool {
	return s.Disabled
}

func (s *Spec) SetRequiredDefaults() {
	s.Ops.StreamsPerPod = DefaultStreamsPerPod
	s.Ops.MicroBatchSize = DefaultMicroBatchSize
	s.Ops.MicroBatchBytes = DefaultMicroBatchBytes
	s.Ops.MicroBatchTimoutMs = DefaultMicroBatchTimeoutMs
	s.Ops.MaxEventProcessingRetries = DefaultMaxEventProcessingRetries
	s.Ops.MaxStreamRetryBackoffIntervalSec = DefaultMaxStreamRetryBackoffIntervalSec
	s.Ops.HandlingOfUnretryableEvents = HoueDefault
}

type Ops struct {
	// StreamsPerPod specifies how many Executors that should execute the stream concurrently
	// in its own Goroutine.
	// This is especially important when using Kafka extractors. For max concurrency and highest throughput
	// it should be set equal to the number of topic partitions divided by expected number of pods.
	// There is negligible overhead in having more goroutines than partitions.
	// If omitted it is set to DefaultStreamsPerPod (1).
	StreamsPerPod int `json:"streamsPerPod"`

	// MicroBatch specifies if events should be processed in batches, which improves throughput.
	// If set to 'true' the involved stream entities try their best to process events in batches according
	// to each ETL entity's capability for micro-batch processing.
	// If omitted or set to false, the stream will process a single event at a time.
	// Note: This is an optional feature for each Source/Extractor plugin. See doc for each plugin entity for availability.
	// Example of a plugin supporting this is 'kafka'.
	// Verified beneficial effect is when having sink set to 'bigquery', due to BQ API capabilities for this.
	MicroBatch bool `json:"microBatch"`

	// MicroBatchSize is the maximum number of events that should be included in the batch.
	// If omitted it is set to DefaultMicroBatchSize
	MicroBatchSize int `json:"microBatchSize"`

	// MicroBatchSize specifies the threshold that when reached closes the batch regardless of number of events in
	// the batch, and forwards it downstream. The final size of the batch will be this threshold + size of next event.
	// If omitted it is set to DefaultMicroBatchSizeBytes
	MicroBatchBytes int `json:"microBatchBytes"`

	// MicroBatchTimeout is the maximum time to wait for the batch to fill up if the max size has not been reached.
	// If the sink is set to Kafka, this value will override the pollTimeout value.
	// If omitted it is set to DefaultMicroBatchTimeoutMs
	MicroBatchTimoutMs int `json:"microBatchTimeoutMs"`

	// MaxEventProcessingRetries specifies how many times an extracted event from the source should be processed
	// again (transform/load), if deemed retryable, before the Executor restarts the stream on a longer back-off
	// interval (MaxStreamRetryBackoffInterval). Retryable errors will be retried indefinitely for max self-healing.
	// If omitted it is set to DefaultMaxEventProcessingRetries.
	MaxEventProcessingRetries int `json:"maxEventProcessingRetries"`

	// MaxStreamRetryBackoffInterval specifies the max time between stream restarts after exponential backoff
	// retries of retryable event processing failures.
	// If omitted it is set to DefaultMaxStreamRetryBackoffInterval
	MaxStreamRetryBackoffIntervalSec int `json:"maxStreamRetryBackoffIntervalSec"`

	// HandlingOfUnretryableEvents specifies what to do with events that can't be properly transformed or loaded
	// to the sink, e.g. corrupt or otherwise non-compliant events vs the stream spec.
	// Available options are:
	//
	//		"default" - Default behaviour depending on Extractor type. For Kafka this means "discard" and for
	//					Pubsub it means Nack (continue retrying later but process other events as well).
	//					If this field is omitted it will take this value.
	//
	//		"discard" - Discard the event, log it with Warn, and continue processing other events.
	//
	//		"dlq"     - Move the event from the source topic to a DLQ topic named "geist.dlq.<streamId>".
	//
	//		"fail"    - The stream will be terminated with an error message.
	//
	// Note that all sink types might not support all available options. See documentation for each sink type for details.
	//
	HandlingOfUnretryableEvents string `json:"handlingOfUnretryableEvents"`

	// LogEventData is useful for enabling granular event level debugging dynamically for specific streams
	// without having to redeploy GEIST. To troubleshoot a specific stream a new version of the stream spec
	// can be uploaded at run-time (e.g. with LSD) with this field set to true.
	LogEventData bool `json:"logEventData"`
}

//
// Source spec
//
type Source struct {
	Type   EntityType   `json:"type"`
	Config SourceConfig `json:"config"`
}

type SourceConfig struct {
	Provider     string        `json:"provider,omitempty"` // Available options (only used for Kafka Source): "native" and "confluent"
	Topics       []Topics      `json:"topics,omitempty"`
	Subscription *Subscription `json:"subscription,omitempty"`

	// PollTimeoutMs is a Kafka consumer specific property, specifying after how long time to return from the Poll()
	// call, if no messages are available for consumption. If this is omitted the value will be set to GEIST config
	// default (app.kafka.pollTimeoutMs). Normally this is not needed to be provided in the stream spec, nor changed
	// in the config. It has no impact on throughput. A higher value will lower the cpu load on idle streams.
	PollTimeoutMs *int `json:"pollTimeoutMs,omitempty"`

	// MaxOutstandingMessages is a PubSub consumer specific property, specifying max number of fetched but not yet
	// acknowledged messages in pubsub consumer. If this is omitted the value will be set to the loaded Pubsub entity
	// config default.
	// For time consuming transform/sink streams decrease this value while increasing ops.streamsPerPod
	MaxOutstandingMessages *int `json:"maxOutstandingMessages,omitempty"`

	// MaxOutstandingBytes is a PubSub consumer specific property, specifying max size of fetched but not yet
	// acknowledged messages.
	MaxOutstandingBytes *int `json:"maxOutstandingBytes,omitempty"`

	// Synchronous is a PubSub consumer specific property, that can be used to tune certain type of streams (e.g.
	// spiky input flow of messages with very heavy transforms or slow sinks, where setting this to true could
	// reduce number of expired messages. Default is false.
	Synchronous *bool `json:"synchronous,omitempty"`

	// NumGoroutines is a PubSub consumer specific property used for increasing rate of incoming messages in case
	// downstream ETL is not cpu starved or blocked on sink ops, while Extractor cannot keep up with consuming
	// incoming messages. Depending on type of Sink/Loader a better/alternative approach is to increase ops.streamsPerPod.
	// If omitted it is set to 1.
	NumGoroutines *int `json:"numGoroutines,omitempty"`

	// Properties holds direct low-level entity properties like Kafka consumer props
	Properties []Property `json:"properties,omitempty"`

	// CustomConfig can be used by custom source/sink plugins for config options not explicitly provided by the Spec struct
	CustomConfig any `json:"customConfig,omitempty"`
}

type Topics struct {
	// Env specifies for which environment/stage the topic names config should be used.
	// Allowed values are "all" or any string matching the config provided to registered entity factories.
	// Normally, "dev", "stage", and "prod" is used.
	Env   Environment `json:"env,omitempty"`
	Names []string    `json:"names,omitempty"`
}

type Subscription struct {
	// Type can be:
	//
	// 		"shared" - meaning multiple consumers share this subscription in a competing consumer pattern.
	//				   Only one of the subscribers will receive each event.
	//				   If this is set, the name of the subscription needs to be present in the "Name" field.
	//
	//		"unique" - meaning each transloading stream instance will have its own unique subscription.
	//				   All instances will thus get all events from the topic.
	//				   If this is set, a unique subscription name will be created and the Name field is
	//				   ignored. This one is used internally by each pod's Supervisor to receive notifications
	//                 about registry updates, from other Supervisors' registry instances.
	Type string `json:"type,omitempty"`

	Name string `json:"name,omitempty"`
}

type Property struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

//
// Transform spec
//
type Transform struct {
	// ImplId denotes the Implementation ID (type of Transform implementation).
	// The GEIST built-in type is named 'native' and is currently the only one supported
	// TODO: Change this name to type? or just id?
	ImplId EntityType `json:"implId,omitempty"`

	// ExcludeEventsWith will be checked first to exclude events, matching conditions, from all other transformations.
	// If multiple filter objects are provided they are handled as OR type of filters.
	ExcludeEventsWith []ExcludeEventsWith `json:"excludeEventsWith,omitempty"`

	// The ExtractFields transformation type picks out fields from the input event JSON.
	// The first ExtractFields object that matches the ForEventsWith filter will be used
	// to create the resulting Transformed object.
	ExtractFields []ExtractFields `json:"extractFields,omitempty"`

	ExtractItemsFromArray []ExtractItemsFromArray `json:"extractItemsFromArray,omitempty"`

	// AddFieldsToJson can contain a list of extracted fields to be added to a another extracted field,
	// if that field is a JSON.
	AddFieldsToJson AddFieldsToJson `json:"addFieldsToJson,omitempty"`

	// The Regexp transformation transforms a string into a JSON based on the groupings in
	// the regular expression.
	// Minimum one groupings needs to be made.
	Regexp *Regexp `json:"regexp,omitempty"`
}

type AddFieldsToJson struct {
	JsonId      string
	FieldsToAdd []string
}

// ExcludeEventsWith specifies if certain events should be skipped directly, without further processing.
// If the event field as specified by the Key field matches any of the values in the Values array
// the event will be excluded.
// The Key string must be on a JSON path syntax according to github.com/tidwall/gjson (see below).
// The value field is currently limited to string values.
type ExcludeEventsWith struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

// The ExtractFields transformation type creates root level ID fields, with values
// retrieved from a json path expression from the input event
type ExtractFields struct {
	// ForEventsWith is used to filter which incoming event the fields should be extracted from
	// Currently only AND type filter is supported if supplying multiple key-value pairs.
	// If ForEventsWith is empty or omitted, fields will be taken from all events.
	ForEventsWith []ForEventsWith `json:"forEventsWith,omitempty"`

	// The Fields a array contains the definitions of which fields to extract for the filtered-out event
	Fields []Field `json:"fields,omitempty"`
}

// ExtractItemsFromArray transformation returns all items in an arbitrary array inside the event json
// with the ID/key of each item according to the required IdFromItemFields spec.
// If the resulting ID/key of each item is an empty string the item will be omitted from the output map.
// The items will be stored inside a map in the transformed output map. It's key is specified by the "Id" field.
type ExtractItemsFromArray struct {
	Id            string          `json:"id"`
	ForEventsWith []ForEventsWith `json:"forEventsWith,omitempty"`
	Items         ArrayItems      `json:"items"`
}

type ArrayItems struct {
	JsonPathToArray  string           `json:"jsonPathToArray"`
	IdFromItemFields IdFromItemFields `json:"idFromItemFields"`
}

type IdFromItemFields struct {
	Delimiter string   `json:"delimiter"`
	Fields    []string `json:"fields"`
}

func (t *Transform) validate() error {
	var err error
	if t.Regexp != nil {
		if t.Regexp.Expression == "" {
			return fmt.Errorf("no RegExp is specified")
		}

		_, err = regexp.Compile(t.Regexp.Expression)
		if err != nil {
			return fmt.Errorf("error during RegExp compile: %v", err.Error())
		}

		groups := t.Regexp.CollectGroups(t.Regexp.Expression)
		if len(groups) < 1 {
			return fmt.Errorf("no groupings where found in regular expression %s", t.Regexp.Expression)
		}

		if t.Regexp.TimeConversion != nil {
			if len(t.Regexp.TimeConversion.Field) < 1 {
				return fmt.Errorf("regexp.timeConversion.field must be set")
			}
			if len(t.Regexp.TimeConversion.InputFormat) < 1 {
				return fmt.Errorf("regexp.timeConversion.inputFormat must be set")
			}
		}
	}

	// Further add validations for transform

	return nil
}

type Regexp struct {
	// The regular expression, in RE2 syntax.
	Expression string `json:"expression,omitempty"`

	// If used in conjunction with fieldExtraction, this will be the field to apply regexp on.
	Field string `json:"field,omitempty"`

	// If extracted field should be kept in result or omitted. Default is false.
	KeepField bool `json:"keepField,omitempty"`

	// Time conversion of date field. Field specified must be extracted before.
	TimeConversion *TimeConv `json:"timeConversion,omitempty"`
}

// TODO: Shorten, but not important now since its no performance impact.
func (r *Regexp) CollectGroups(exp string) []string {

	var (
		str    string
		groups []string
	)

	for i := 0; i < len(exp); i++ {
		if strings.EqualFold(string(exp[i]), "<") {
			for i = i + 1; ; i++ {
				if strings.EqualFold(string(exp[i]), ">") {
					break
				}
				str += string(exp[i])
			}
			groups = append(groups, str)
			str = ""
		}
	}

	return groups
}

type TimeConv struct {
	// Field where the data is located and should be converted.
	Field string `json:"field,omitempty"`

	// Input format of date to be converted. Mandatory.
	InputFormat string `json:"inputFormat,omitempty"`

	// Output format of date, if omitted, ISO-8601 is used.
	OutputFormat string `json:"outputFormat,omitempty"`
}

// The Key string must be on a JSON path syntax according to github.com/tidwall/gjson (see below).
// The value field is currently limited to string values.
type ForEventsWith struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Field struct {
	Id string `json:"id"`

	// JsonPath defines which field in the JSON that should be extracted. It uses github.com/tidwall/gjson
	// syntax, such as "myCoolField" if we want to extract that field from json { "myCoolField": "isHere" }
	//
	// The full raw JSON event is also regarded as a 'field' and to extract that the JsonPath string should
	// be empty or omitted in the spec.
	JsonPath string `json:"jsonPath"`

	// - For normal fields, Type can be "string", "integer", "number", "boolean" or "float".
	// If omitted in the spec, string will be used.
	//
	// - For raw event fields the default type is []byte, unless Type is explicitly set to "string".
	// For performance critical streams, type should be omitted (avoiding conversions), especially when
	// having a stream with BigTable sink, which stores the data as byte anyway.
	//
	// - If a field is an iso timestamp string (e.g. "2019-11-30T14:57:23.389Z") the type
	// "isoTimestamp" can be used, to have a Go time.Time object created as the value for this field key.
	//
	// - If a field is a unix timestamp (number or str) (e.g. 1571831226950 or "1571831226950") the type
	// "unixTimestamp" can be used to have Go time.Time object created as the value for this field.
	//
	// - If a field is a User Agent string (e.g. "Mozilla%2F5.0%20(Macintosh%3B%20Intel%2...") the type
	// "userAgent" can be used to have parsed JSON output as string, with separate fields for each part of UA.
	Type string `json:"type,omitempty"`
}

//
// Sink spec
//
type Sink struct {
	// Type specifies the type of sink into which data should be loaded.
	// Important stream constraints are noted below for each sink type where needed.
	//
	//		"bigquery" - Each transformed event (to be inserted as a row) should be well below 5MB to avoid
	//                   BigQuery http request size limit of 10MB for streaming inserts.
	//      "kafka"    - Max size of published events are default set to 2MB, but topics can set this higher
	//                   to max 8MB.
	Type EntityType `json:"type"`

	Config *SinkConfig `json:"config,omitempty"`
}

type SinkConfig struct {
	Provider string      `json:"provider,omitempty"` // Available options (only used for Kafka sink): "native" and "confluent"
	Topic    []SinkTopic `json:"topic,omitempty"`
	Message  *Message    `json:"message,omitempty"`
	Tables   []Table     `json:"tables,omitempty"`
	Kinds    []Kind      `json:"kinds,omitempty"` // TODO: Probably remove array and keep single object

	// Synchronous is used by Kafka sink/loader to specify if ensuring each event is guaranteed to be persisted to
	// broker (Synchronous: true), giving lower throughput (without not yet provided batch option), or if verifying
	// delivery report asynchronously (Synchronous: false), giving much higher throughput, but could lead to
	// message loss if GEIST crashes.
	Synchronous *bool `json:"synchronous,omitempty"`

	// DiscardInvalidData specifies if invalid data should be prevented from being stored in the sink and instead
	// logged and discarded.
	// It increases CPU load somewhat but can be useful to enable in case of data from an unreliable source is
	// being continuously retried and where the stream's HandlingOfUnretryableEvents mode is not granular enough.
	// One example is when having the MicroBatch mode enabled and we want to just discard individual invalid
	// events, instead of retrying or DLQ:ing the whole micro batch.
	// It is currently only regarded when using the BigQuery sink.
	DiscardInvalidData bool `json:"discardInvalidData,omitempty"`

	// Direct low-level entity properties like Kafka producer props
	Properties []Property `json:"properties,omitempty"`

	// CustomConfig can be used by custom source/sink plugins for config options not explicitly provided by the Spec struct
	CustomConfig any `json:"customConfig,omitempty"`
}

type SinkTopic struct {
	Env       Environment         `json:"env,omitempty"`
	TopicSpec *TopicSpecification `json:"topicSpec,omitempty"`
}

// Name, NumPartitions and ReplicationFactor are required.
// If sink topic is referring to an existing topic only Name will be used.
type TopicSpecification struct {
	Name              string            `json:"name"`
	NumPartitions     int               `json:"numPartitions"`
	ReplicationFactor int               `json:"replicationFactor"`
	Config            map[string]string `json:"config,omitempty"` // not yet supported
}

// Message is used for sinks like PubSub and Kafka, specifying how the message should be published
type Message struct {
	// PayloadFromId is the key/field ID in the Transformed output map, which contains the actual message payload
	PayloadFromId string `json:"payloadFromId,omitempty"`
}

// The Kind struct is used for Firestore sinks (in datastore mode).
// Currently, one of EntityName or EntityNameFromIds needs to be present in spec.
// TODO: Add creation of UUID if EntityName/Ref not present
type Kind struct {
	// If Namespace here is present, it will override the global one.
	// If both are missing, the Kind will use native 'default'
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`

	// If set, will be used as the actual Entity Name
	EntityName string `json:"entityName,omitempty"`

	// If set, will be used to create the Entity Name from the "id" values in the Transload output map.
	// The value is currently restricted to be of type string.
	EntityNameFromIds struct {
		Ids       []string `json:"ids,omitempty"`
		Delimiter string   `json:"delimiter,omitempty"`
	} `json:"entityNameFromIds,omitempty"`

	Properties []EntityProperty `json:"properties,omitempty"`
}

type EntityProperty struct {
	Name string `json:"name"`

	// Id is the key/field ID in the Transformed output map, which contains the actual value
	// for this property. The value type is the same as the output from the Transform.
	Id string `json:"id"`

	// For most properties this should be set to true, for improved query performance, but for big event
	// fields that might exceed 1500 bytes, this should be set to false, since that is a built-in
	// Firestore limit.
	Index bool `json:"index"`
}

// The Table struct is used for BigTable, BigQuery and other table based sinks.
type Table struct {
	Name string `json:"name"`

	// Dataset is optional depending on sink type. Currently only used by BigQuery.
	Dataset string `json:"dataset"`

	// DatasetCreation is only required if the dataset is meant to be created by this stream
	// *and* if other values than the default ones are required.
	// Default values are location: EU and empty description.
	DatasetCreation *DatasetCreation `json:"datasetCreation,omitempty"`

	// Table spec for SQL type sinks such as BigQuery
	Columns       []Column       `json:"columns"`
	TableCreation *TableCreation `json:"tableCreation,omitempty"`

	// InsertIdFromId defines which value in the Transformed output map will contain the insert ID,
	// as extracted from one of the input event fields.
	// The value referred to in the transloaded output map needs to be of string type.
	// This is used for BigQuery best-effort deduplication.
	InsertIdFromId string `json:"insertIdFromId"`

	// Table spec for BigTable are built up by RowKey and ColumnFamilies
	RowKey         RowKey         `json:"rowKey"`
	ColumnFamilies []ColumnFamily `json:"columnFamilies"`

	// Only input transformations satisfying the whitelist key/value filter will be
	// processed by the sink (mostly needed in multi-table Sink specs)
	Whitelist *Whitelist `json:"whitelist,omitempty"`
}

type Column struct {
	// Name of the column as specified at spec registration time.
	// One of Name or NameFromId needs to be present in the column spec.
	Name string `json:"name"`

	// If NameFromId is non-nil columns will be generated dynamically based on transformation output.
	// The name of the column will be set to the value in the Transformed map, with the key as found in NameFromId.
	// Note that the field fetched from the event, to be the column name, need to be of string type.
	NameFromId *NameFromId `json:"nameFromId,omitempty"`

	// Mode uses the definitions as set by BigQuery with "NULLABLE", "REQUIRED" or "REPEATED"
	Mode string `json:"mode"`

	// Type uses the BigQuery Standard SQL types.
	// The type here needs to match the one used in the Transform extract field spec.
	// For date/time/timestamp types the type used in the Transform extract field spec needs to be set to
	// "isoTimestamp" or "unixTimestamp".
	Type string `json:"type"`

	Description string   `json:"description"`
	Fields      []Column `json:"fields"` // For nested columns

	// ValueFromId is not part of schema definition per se, but specifies what value from the incoming
	// transformed data that should be inserted here.
	// A special value can be set to have a column with GEIST ingestion time, which could be used together
	// with TimePartitioning config, as an alternative to the also available default BQ insert partitioning.
	// To enable this, the field should be set to "@geistIngestionTime", with column type set to "TIMESTAMP"
	// and mode set to "NULLABLE".
	ValueFromId string `json:"valueFromId"`
}

// Creates a Column/CQ name from id outputs in transloaded event map
type NameFromId struct {
	Prefix       string `json:"prefix"`
	SuffixFromId string `json:"suffixFromId"`

	// Preset contains a list of Column/CQ names that will be added to table directly during table creation.
	// This is not support (not needed) by BigTable loader, only BigQuery loader.
	Preset []string `json:"preset,omitempty"`
}

// DatasetCreation config contains table creation details.
// It is currently only used by BigQuery sinks.
type DatasetCreation struct {
	Description string `json:"description"`

	// Geo location of dataset.
	// Valid values are:
	// EU
	// europe
	// US
	// plus all regional ones as described here: https://cloud.google.com/bigquery/docs/locations
	// If omitted or empty the default location will be set to EU.
	Location string `json:"location"`
}

// TableCreation config contains table creation details.
// It is currently only used by BigQuery sinks and most of the fields/comments in the struct are
// copied directly from BQ client, with modifications to fit with the GEIST spec format.
type TableCreation struct {
	Description string `json:"description"`

	// If non-nil, the table is partitioned by time. Only one of
	// time partitioning or range partitioning can be specified.
	TimePartitioning *TimePartitioning `json:"timePartitioning,omitempty"`

	// If non-nil, the table is partitioned by integer range.  Only one of
	// time partitioning or range partitioning can be specified.
	//RangePartitioning *RangePartitioning

	// If set to true, queries that reference this table must specify a
	// partition filter (e.g. a WHERE clause) that can be used to eliminate
	// partitions. Used to prevent unintentional full data scans on large
	// partitioned tables.
	RequirePartitionFilter bool `json:"requirePartitionFilter"`

	// Clustering specifies the data clustering configuration for the table.
	Clustering []string `json:"clustering,omitempty"`

	// The time when this table expires. If set, this table will expire at the
	// specified time. Expired tables will be deleted and their storage
	// reclaimed. The zero value is ignored.
	//ExpirationTime time.Time

	// User-provided labels.
	//Labels map[string]string
}

// TimePartitioning describes the time-based date partitioning on a table.
// It is currently only used by BigQuery sinks and most of the fields/docs in the struct are
// copied directly from BQ client, with modifications to fit with the GEIST spec format.
// For more information see: https://cloud.google.com/bigquery/docs/creating-partitioned-tables.
type TimePartitioning struct {
	// Defines the partition interval type. Supported values are "DAY" or "HOUR".
	Type string `json:"type"`

	// The amount of hours to keep the storage for a partition.
	// If the duration is empty (0), the data in the partitions do not expire.
	ExpirationHours int `json:"expirationHours"`

	// If empty, the table is partitioned by pseudo column '_PARTITIONTIME'; if set, the
	// table is partitioned by this field. The field must be a top-level TIMESTAMP or
	// DATE field. Its mode must be NULLABLE or REQUIRED.
	Field string `json:"field"`
}

// RowKey specifies how the row-key should be generated for BigTable sinks.
// If one of the Predefined options are set, that will be used.
// Currently available Predefined options are:
//		"timestampIso"
//		"invertedTimestamp"
//		"uuid"
//		"keysInMap"
// If Predefined is not set, the Keys array should be used to specify which extracted fields
// from the event should be used.
// TODO: Add padding config
type RowKey struct {
	Predefined string   `json:"predefined,omitempty"`
	Keys       []string `json:"keys,omitempty"`
	Delimiter  string   `json:"delimiter,omitempty"`

	// Only required when using the Predefined rowkey option "keysInMap". This id should map to the transformed
	// output map item specified in ExtractItemsFromArray.Id
	MapId string `json:"mapId,omitempty"`
}

type Whitelist struct {
	Id     string   `json:"id"`
	Type   string   `json:"type"`
	Values []string `json:"values"`
}

type ColumnFamily struct {
	Name                    string                   `json:"name"`
	GarbageCollectionPolicy *GarbageCollectionPolicy `json:"garbageCollectionPolicy"`
	ColumnQualifiers        []ColumnQualifier        `json:"columnQualifiers"`
}

// TODO: Add support for Intersection and Union policies
// The following types are supported:
// - MaxVersions: where Value takes an integer of number of old versions to keep (-1)
// - MaxAge: where Value takes an integer of number of hours before deleting the data.
type GarbageCollectionPolicy struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

// The Id field can be used directly in the Transformed map to fetch the value to be inserted
// The Name field is the actual CQ name to be used in the table.
// Either Name or NameFromId must be present, not both.
type ColumnQualifier struct {
	Id         string      `json:"id"`
	Name       string      `json:"name,omitempty"`
	NameFromId *NameFromId `json:"nameFromId,omitempty"`
}

// Stream spec JSON schema validation will be handled by NewSpec() using validateRawJson() against geist spec json schema.
// This func contains more complex validation such as Regexp validation.
func (s *Spec) Validate() error {

	return s.Transform.validate()
}

func (s *Spec) JSON() []byte {
	specData, _ := json.Marshal(s)
	return specData
}

func validateRawJson(specData []byte) error {
	schemaLoader := gojsonschema.NewBytesLoader(specSchema)
	documentLoader := gojsonschema.NewBytesLoader(specData)
	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return err
	}

	if !result.Valid() {
		specErrors := ""
		for _, desc := range result.Errors() {
			specErrors += " - " + desc.String()
		}
		err = errors.New(specErrors)
	}
	return err
}

// Initial stream spec schema with only the most important checks. Will be more detailed later.
var specSchema = []byte(`
{
   "$schema": "http://json-schema.org/draft-07/schema",
   "type": "object",
   "required": [
      "namespace",
      "streamIdSuffix",
      "version",
      "description",
      "source",
      "transform",
      "sink"
   ],
   "properties": {
      "namespace": {
         "type": "string",
         "minLength": 1
      },
      "streamIdSuffix": {
         "type": "string",
         "minLength": 1
      },
      "version": {
         "type": "integer"
      },
      "description": {
         "type": "string",
         "minLength": 1
      },
      "disabled": {
         "type": "boolean"
      },
      "ops": {
         "type": "object",
         "properties": {
            "streamsPerPod": {
               "type": "integer"
            },
            "microBatch": {
               "type": "boolean"
            },
            "microBatchSize": {
               "type": "integer"
            },
            "microBatchBytes": {
               "type": "integer"
            },
            "microBatchTimeoutMs": {
               "type": "integer"
            },
            "maxEventProcessingRetries": {
               "type": "integer"
            },
            "maxStreamRetryBackoffIntervalSec": {
               "type": "integer"
            },
            "handlingOfUnretryableEvents": {
               "type": "string",
               "enum": [
                  "default",
                  "discard",
                  "dlq",
                  "fail"
               ]
            },
            "logEventData": {
               "type": "boolean"
            }
         },
         "additionalProperties": false
      },
      "source": {
         "type": "object",
         "required": [
            "type"
         ],
         "properties": {
            "type": {
               "type": "string",
               "minLength": 1
            },
            "config": {
               "type": "object",
               "properties": {
                  "provider": {
                     "type": "string",
                     "enum": [
                        "native",
                        "confluent"
                     ]
                  }
               }
            }
         }
      },
      "transform": {
         "type": "object"
      },
      "sink": {
         "type": "object",
         "required": [
            "type"
         ],
         "properties": {
            "type": {
               "type": "string",
               "minLength": 1
            }
         }
      }
   },
   "additionalProperties": false
}
`)
