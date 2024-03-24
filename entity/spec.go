package entity

import (
	"encoding/json"
	"errors"

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

// Spec implements the GEIST Stream Spec interface and specifies how each ETL stream should
// be executed from Source to Transform to Sink. Specs are registered and updated through
// a stream of its own, as specified by the configurable SpecRegistrationSpec.
// The Namespace + StreamIdSuffix combination must be unique (forming a GEIST Stream ID).
// To succeed with an upgrade of an existing spec the version number needs to be incremented.
type Spec struct {
	// Main metadata (required)
	Namespace      string `json:"namespace"`
	StreamIdSuffix string `json:"streamIdSuffix"`
	Description    string `json:"description"`
	Version        int    `json:"version"`

	// Operational config (optional)
	Disabled  bool           `json:"disabled"`
	Ops       Ops            `json:"ops"`
	OpsPerEnv map[string]Ops `json:"opsPerEnv,omitempty"`

	// Stream entity config (required)
	Source    Source    `json:"source"`
	Transform Transform `json:"transform"`
	Sink      Sink      `json:"sink"`
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

	err := json.Unmarshal(specData, &spec)
	if err == nil {
		spec.EnsureValidDefaults()
		err = spec.Validate()
	}
	return &spec, err
}

func NewEmptySpec() *Spec {
	var spec Spec
	spec.EnsureValidDefaults()
	return &spec
}

func (s *Spec) Id() string {
	return s.Namespace + "-" + s.StreamIdSuffix
}

func (s *Spec) IsDisabled() bool {
	return s.Disabled
}

func (s *Spec) EnsureValidDefaults() {
	s.Ops.EnsureValidDefaults()
	for env, ops := range s.OpsPerEnv {
		ops.EnsureValidDefaults()
		s.OpsPerEnv[env] = ops
	}
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
	MicroBatchSize int `json:"microBatchSize,omitempty"`

	// MicroBatchSize specifies the threshold that when reached closes the batch regardless of number of events in
	// the batch, and forwards it downstream. The final size of the batch will be this threshold + size of next event.
	// If omitted it is set to DefaultMicroBatchSizeBytes
	MicroBatchBytes int `json:"microBatchBytes,omitempty"`

	// MicroBatchTimeout is the maximum time to wait for the batch to fill up if the max size has not been reached.
	// If the sink is set to Kafka, this value will override the pollTimeout value.
	// If omitted it is set to DefaultMicroBatchTimeoutMs
	MicroBatchTimeoutMs int `json:"microBatchTimeoutMs,omitempty"`

	// MaxEventProcessingRetries specifies how many times an extracted event from the source should be processed
	// again (transform/load), if deemed retryable, before the Executor restarts the stream on a longer back-off
	// interval (MaxStreamRetryBackoffInterval). Retryable errors will be retried indefinitely for max self-healing.
	// If omitted it is set to DefaultMaxEventProcessingRetries.
	MaxEventProcessingRetries int `json:"maxEventProcessingRetries"`

	// MaxStreamRetryBackoffInterval specifies the max time between stream restarts after exponential backoff
	// retries of retryable event processing failures.
	// If omitted or zero it is set to DefaultMaxStreamRetryBackoffInterval
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
	//		"dlq"     - Move the event from the source topic to a DLQ topic specified in DLQ Config.
	//
	//		"fail"    - The stream will be terminated with an error message.
	//
	// Note that all source types might not support all available options. See documentation for each source type for details.
	//
	HandlingOfUnretryableEvents string `json:"handlingOfUnretryableEvents,omitempty"`

	// LogEventData is useful for enabling granular event level debugging dynamically for specific streams
	// without having to redeploy GEIST. To troubleshoot a specific stream a new version of the stream spec
	// can be uploaded at run-time with this field set to true.
	LogEventData bool `json:"logEventData"`

	// CustomProperties can be used to configure stream processing in any type of custom
	// connector or injected enrichment logic.
	CustomProperties map[string]string `json:"customProperties"`
}

func (o *Ops) EnsureValidDefaults() {
	if o.StreamsPerPod <= 0 {
		o.StreamsPerPod = DefaultStreamsPerPod
	}
	if o.MicroBatch {
		if o.MicroBatchSize <= 0 {
			o.MicroBatchSize = DefaultMicroBatchSize
		}
		if o.MicroBatchBytes <= 0 {
			o.MicroBatchBytes = DefaultMicroBatchBytes
		}
		if o.MicroBatchTimeoutMs <= 0 {
			o.MicroBatchTimeoutMs = DefaultMicroBatchTimeoutMs
		}
	}
	if o.MaxEventProcessingRetries <= 0 {
		o.MaxEventProcessingRetries = DefaultMaxEventProcessingRetries
	}
	if o.MaxStreamRetryBackoffIntervalSec <= 0 {
		o.MaxStreamRetryBackoffIntervalSec = DefaultMaxStreamRetryBackoffIntervalSec
	}
	if o.HandlingOfUnretryableEvents == "" {
		o.HandlingOfUnretryableEvents = HoueDefault
	}
}

// Source spec
type Source struct {
	Type   EntityType   `json:"type"`
	Config SourceConfig `json:"config"`
}

type SourceConfig struct {

	// Properties is a generic property container
	Properties []Property `json:"properties,omitempty"`

	// CustomConfig can be used by custom source/sink plugins for config options not explicitly provided by the Spec struct
	CustomConfig any `json:"customConfig,omitempty"`
}

type Property struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Transform spec
type Transform struct {
	// ImplId denotes the Implementation ID (type of Transform implementation).
	// The GEIST built-in type is named 'native' and is currently the only one supported
	// TODO: Change this name to type? or just id?
	ImplId EntityType `json:"implId,omitempty"`

	// ExcludeEventsWith will be checked first to exclude events, matching conditions,
	// from all other transformations. If multiple filter objects are provided they are
	// handled as OR type of filters.
	ExcludeEventsWith []ExcludeEventsWith `json:"excludeEventsWith,omitempty"`

	// The ExtractFields transformation type picks out fields from the input event JSON.
	// The first ExtractFields object that matches the ForEventsWith filter will be used
	// to create the resulting Transformed object.
	ExtractFields []ExtractFields `json:"extractFields,omitempty"`

	ExtractItemsFromArray []ExtractItemsFromArray `json:"extractItemsFromArray,omitempty"`

	// The Regexp transformation transforms a string into a JSON based on the groupings in
	// the regular expression. Minimum one groupings needs to be made.
	Regexp *Regexp `json:"regexp,omitempty"`
}

func (t *Transform) Validate() (err error) {
	if t.Regexp != nil {
		err = t.Regexp.Validate()
	}
	return err
}

// ExcludeEventsWith specifies if certain events should be skipped directly, without further processing.
// If the event field as specified by the Key field matches any of the values in the Values array
// the event will be excluded. This is the Blacklisting option of this filter.
// The Key string must be on a JSON path syntax according to github.com/tidwall/gjson (see below).
// The value field is currently limited to string values.
//
// If Values array is missing or empty a check will be done on ValuesNotIn. If the event field as
// specified by the Key field does not have a value matching any of the values in the ValuesNotIn
// field, the event is excluded.
// This is the Whitelisting option of this filter.
//
// If ValueIsEmpty is set to true and the field string value is empty, the event will be excluded.
type ExcludeEventsWith struct {
	Key          string   `json:"key"`
	Values       []string `json:"values,omitempty"`
	ValuesNotIn  []string `json:"valuesNotIn,omitempty"`
	ValueIsEmpty *bool    `json:"valueIsEmpty,omitempty"`
}

// The ExtractFields transformation type creates root level ID fields, with values
// retrieved from a json path expression from the input event
type ExtractFields struct {
	// ForEventsWith is used to filter which incoming event the fields should be extracted from
	// Currently only AND type filter is supported if supplying multiple key-value pairs.
	// If ForEventsWith is empty or omitted, fields will be taken from all events.
	ForEventsWith []ForEventsWith `json:"forEventsWith,omitempty"`

	// ExcludeEventsWith inside ExtractField complements the top level ExcludeEventsWith by
	// only being applicable for the events filtered with ForEventsWith.
	// As an example, this simplifies event schema evolution whereby a field containing the
	// schema version number could be governed by ForEventsWith, and event exclusion (and
	// field extraction) are handled by this and subsequent specification constructs.
	ExcludeEventsWith []ExcludeEventsWith `json:"excludeEventsWith,omitempty"`

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

// The Key string must be on a JSON path syntax according to github.com/tidwall/gjson (see below).
// Note that while the 'Value' field in the ForEventsWith spec is of string type, the actual field
// in the incoming event can be of for example int type in addition to string, where a match will
// be made of its string representation. For example, if 'Value' is set to "3" and the field in the
// incoming event is of JSON number type (int) with a value of 3, a match will be made correctly.
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

// Sink spec
type Sink struct {
	Type   EntityType  `json:"type"`
	Config *SinkConfig `json:"config,omitempty"`
}

type SinkConfig struct {

	// Generic property container
	Properties []Property `json:"properties,omitempty"`

	// CustomConfig can be used by custom source/sink plugins for config options not explicitly provided by the Spec struct
	CustomConfig any `json:"customConfig,omitempty"`
}

// Stream spec JSON schema validation will be handled by NewSpec() using validateRawJson() against
// Geist spec json schema. This method enables more complex validation such as Regexp validation.
func (s *Spec) Validate() error {
	return s.Transform.Validate()
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

// Stream spec schema for validation purposes
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
      "$ref": "#/$defs/ops"
    },
    "opsPerEnv": {
      "anyOf": [
        {
          "type": "object",
          "additionalProperties": {
            "$ref": "#/$defs/ops"
          }
        },
        {
          "type": "null"
        }
      ]
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
  "additionalProperties": false,
  "$defs": {
    "mapString": {
      "type": "string"
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
        },
        "customProperties": {
          "anyOf": [
            {
              "type": "object",
              "additionalProperties": {
                "$ref": "#/$defs/mapString"
              }
            },
            {
              "type": "null"
            }
          ]
		}
      },
      "additionalProperties": false
    }
  }
}
`)
