// Package eventsim enables deploying event simulation streams, by setting the "source"
// field in the stream spec to "eventsim".
package eventsim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/teltech/logger"
	"github.com/tidwall/sjson"
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/pkg/notify"
)

const (
	SourceTypeId                     = "eventsim"
	DefaultMaxFractionDigits         = 2
	DefaultSimResolutionMilliseconds = 5000
	EventGenTypeRandom               = "random"
	EventGenTypeSinusoid             = "sinusoid"
	TimestampLayoutIsoSeconds        = "2006-01-02T15:04:05Z"
	TimestampLayoutIsoMillis         = "2006-01-02T15:04:05.000Z"
	TimestampLayoutIsoMicros         = "2006-01-02T15:04:05.000000Z"
)

var DefaultCharset = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

// SourceSpec specifies the schema of the source part of the stream spec
type SourceSpec struct {
	// The trigger interval for each event generation execution. If set to 0 or omitted
	// in the spec, it will be set to DefaultSimResolutionMilliseconds.
	SimResolutionMilliseconds int `json:"simResolutionMilliseconds"`

	// Event schema and field generation specifications
	EventGeneration EventGeneration `json:"eventGeneration"`
	EventSpec       EventSpec       `json:"eventSpec"`
}

// EventGeneration specifies how many events should be generated for each trigger.
//
// "Type" can be one of the following:
//
//	"random"   --> random value between "MinCount" and "MaxCount"
//	"sinosoid" --> the number of events generated over time has a sine wave form with
//	               period specified in "PeriodSeconds", peak-to-peak amplitude in
//	               "MaxCount" - "MinCount", and the timestamp for a peak time in "PeakTime"
//	               (required layout: TimestampLayoutIsoSeconds). To achieve a less perfect
//	               wave, use the Jitter option with a specified randomized timestamp field.
//	""         --> If empty string (or json field omitted) a single event will be
//	               generated for each sim resolution trigger.
type EventGeneration struct {
	Type          string `json:"type"`
	MinCount      int    `json:"minCount"`
	MaxCount      int    `json:"maxCount"`
	PeriodSeconds int    `json:"periodSeconds"`
	PeakTime      string `json:"peakTime"`
}

// EventSpec specifies the event schema
type EventSpec struct {
	Fields []FieldSpec `json:"fields"`
}

// FieldSpec specifies how each field should be generated
type FieldSpec struct {

	// Name of the field on sjson format (see github.com/tidwall/sjson)
	Field string `json:"field"`

	// One of the below options can be present in each field spec
	PredefinedValues []PredefinedValue `json:"predefinedValues"`
	RandomizedValue  *RandomizedValue  `json:"randomizedValue"`
	SetOfStrings     *SetOfStrings     `json:"setOfStrings"`
}

// PredefinedValue enables a field to have one of many provided values set with a probability
// based on the FrequencyFactor.
type PredefinedValue struct {

	// Value can be any json scalar value (string, number, boolean, null)
	Value any `json:"value"`

	// FrequencyFactor specifies the probability of each provided pre-defined value
	// to be set. Any value can be used here, but obviously having the sum for all
	// items being 10 or 100 is the easiest for achieving expected distribution.
	FrequencyFactor int `json:"frequencyFactor"`
}

// RandomizedValue generates a random value for a field.
type RandomizedValue struct {

	// Type is mandatory and have the following supported values:
	//
	//     "int", "integer"
	//     "float"
	//     "string"
	//     "bool"
	//     "isoTimestampMilliseconds"
	//     "isoTimestampMicroseconds"
	//     "uuid"
	//
	// Any other value will lead to an error notification on the notify chan.
	//
	Type string `json:"type"`

	// Min and Max specifies the range of the randomized value
	Min float64 `json:"min"`
	Max float64 `json:"max"`

	// Charset is only applicable for "string" type and specifies which character set
	// to use for string generation. If omitted a default character set will be used.
	// If a value is provided here it needs to have a matching character set added as
	// part of factory creation input config.
	Charset string `json:"charset"`

	// MaxFractionDigits is only applicable for "float" type and specifies how many
	// fraction digits should be provided. If omitted DefaultMaxFractionDigits will
	// be used.
	MaxFractionDigits int `json:"maxFractionDigits"` // only applicable for float types

	// JitterMilliseconds is only applicable for the timestamp types and adds a +-delta
	// duration to the current timestamp, based on a random value from 0 to JitterMilliseconds.
	JitterMilliseconds int `json:"jitterMilliseconds"`
}

// SetOfStrings generates a set of string values from which a random value will be assigned
// to the field. It has similar functionality as PredefinedValues but with two differences:
// 1) Convenient when the number of wanted predefined values becomes very high, e.g. simulating
// high cardinality dimensions. 2) It only supports string values.
//
// The format of the generated strings is "<prefix>n" where 'n' is a number from 1 to "Amount".
//
// If FrequencyMin and FrequencyMax is omitted or set to 0 (or with invalid values), all
// the generated string values will have equal frequency factor (weight) when being randomly
// chosen as the value for the field. Otherwise a random value will be given to the string
// value to make them occur in the events with different frequency, according to the specified
// frequency range.
//
// Field values that should not be present in the generated events must be specified in the
// ExcludeValues slice.
type SetOfStrings struct {
	Amount        int      `json:"amount"`
	Prefix        string   `json:"prefix"`
	FrequencyMin  int      `json:"frequencyMin"`
	FrequencyMax  int      `json:"frequencyMax"`
	ExcludeValues []string `json:"excludeValues"`
}

type ExtractorFactory struct {
	charsets map[string][]rune // Provided custom character sets for random string generation
}

func NewExtractorFactory(charsets map[string][]rune) entity.ExtractorFactory {
	return &ExtractorFactory{charsets: charsets}
}

func (ef *ExtractorFactory) SourceId() string {
	return SourceTypeId
}

func (ef *ExtractorFactory) NewExtractor(ctx context.Context, c entity.Config) (entity.Extractor, error) {
	return newEventSim(c, ef.charsets)
}

func (ef *ExtractorFactory) Close(ctx context.Context) error {
	return nil
}

// eventSim is the source extractor type executing the event sim logic
type eventSim struct {
	config          entity.Config
	sourceSpec      SourceSpec
	notifier        *notify.Notifier
	charsets        map[string][]rune
	frequencyRanges map[string][]FieldFrequencyRange
	peakTime        time.Time
}

func newEventSim(c entity.Config, charsets map[string][]rune) (*eventSim, error) {
	var log *logger.Log
	if c.Log {
		log = logger.New()
	}

	notifier := notify.New(c.NotifyChan, log, 2, "eventsim", c.ID, c.Spec.Id())
	sourceSpec, err := newSourceSpec(c.Spec)
	if err != nil {
		return nil, err
	}

	eventSim := &eventSim{
		config:     c,
		sourceSpec: *sourceSpec,
		notifier:   notifier,
		charsets:   charsets,
	}
	err = eventSim.prepareSimData()
	return eventSim, err
}

func newSourceSpec(spec *entity.Spec) (*SourceSpec, error) {
	var ss SourceSpec
	if spec == nil {
		return nil, errors.New("the provided stream spec must not be nil")
	}

	customConfig, ok := spec.Source.Config.CustomConfig.(map[string]any)
	if !ok {
		return nil, errors.New("invalid stream spec, the 'source.config.customConfig' object was not present")
	}

	b, err := json.Marshal(customConfig)
	if err != nil {
		return nil, fmt.Errorf("invalid source spec provided: %v", customConfig)
	}

	if err = json.Unmarshal(b, &ss); err != nil {
		return nil, err
	}

	if ss.SimResolutionMilliseconds == 0 {
		ss.SimResolutionMilliseconds = DefaultSimResolutionMilliseconds
	}

	adjustSourceSpec(&ss)
	return &ss, validateSpecFields(ss)
}

func adjustSourceSpec(ss *SourceSpec) {
	fields := GenerateFieldsFromSetOfStringsSpec(ss.EventSpec.Fields)
	if len(fields) > 0 {
		ss.EventSpec.Fields = append(ss.EventSpec.Fields, fields...)
	}
}

func validateSpecFields(ss SourceSpec) error {
	if ss.EventGeneration.Type == EventGenTypeRandom || ss.EventGeneration.Type == EventGenTypeSinusoid {
		if ss.EventGeneration.MinCount < 0 || ss.EventGeneration.MaxCount < 0 {
			return errors.New("minCount and maxCount cannot be negative in eventGeneration spec")
		}
		if ss.EventGeneration.MinCount > ss.EventGeneration.MaxCount {
			return errors.New("minCount cannot be higher than maxCount in eventGeneration spec")
		}
	}
	if ss.EventGeneration.Type == EventGenTypeSinusoid {
		if ss.EventGeneration.PeriodSeconds <= 0 {
			return errors.New("periodSeconds must be positive in eventGeneration spec")
		}
	}
	return nil
}

// prepareSimData handles all calculations that can be done prior to actual eventSim
// execution, to reduce CPU load.
func (e *eventSim) prepareSimData() (err error) {
	e.frequencyRanges = createFrequencyRanges(e.sourceSpec.EventSpec.Fields)
	e.peakTime, err = createSinusoidPeakTime(e.sourceSpec.EventGeneration.PeakTime)
	if err != nil {
		return err
	}
	return err
}

// createSinusoidPeakTime returns a single timestamp (of of many) that specifies when
// the event count sinosoid has its highest count value.
func createSinusoidPeakTime(peakTime string) (t time.Time, err error) {
	if peakTime == "" {
		return t, err
	}
	t, err = time.Parse(TimestampLayoutIsoSeconds, peakTime)
	if err != nil {
		return t, err
	}
	// Ensure peak time is before current time by setting the year to last year
	nowLastYear := time.Now().AddDate(-1, 0, 0)
	safePeakTime := time.Date(nowLastYear.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
	return safePeakTime, nil
}

func (e *eventSim) StreamExtract(
	ctx context.Context,
	reportEvent entity.ProcessEventFunc,
	err *error,
	retryable *bool) {

	var events []entity.Event
	rand.Seed(time.Now().UnixNano())
	for {
		sleepCtx(ctx, time.Duration(e.sourceSpec.SimResolutionMilliseconds)*time.Millisecond)

		// An Extractor should check if its context has been cancelled, e.g. due to service shutting down
		if ctx.Err() == context.Canceled {
			e.notifier.Notify(entity.NotifyLevelInfo, "context canceled in StreamExtract")
			*retryable = false
			return
		}

		events, *err = e.createEvents()
		if *err != nil {
			e.notifier.Notify(entity.NotifyLevelError, "failed creating events, err: %s", *err)
			continue
		}

		result := reportEvent(ctx, events)
		*err = result.Error
		*retryable = result.Retryable

		switch result.Status {
		case entity.ExecutorStatusSuccessful:
		case entity.ExecutorStatusShutdown:
			e.notifier.Notify(entity.NotifyLevelInfo, "reportEvent returned ExecutorStatusShutdown, eventSim will initiate shut down")
			return

		// Since eventSim is a source extractor used for test purposes, the following
		// three error conditions will be reported via notifier, while the event generation
		// will continue to operate.
		case entity.ExecutorStatusError:
			e.notifier.Notify(entity.NotifyLevelError, "error from executor in reportEvent(), nb events sent: %d, err: %s, result: %+v", len(events), result.Error, result.Status)
		case entity.ExecutorStatusRetriesExhausted:
			e.notifier.Notify(entity.NotifyLevelError, "reportEvent() returned ExecutorStatusRetriesExhausted, retrying operation once more")
		default:
			e.notifier.Notify(entity.NotifyLevelError, "invalid response from executor in reportEvent(), result: %+v", result.Status)
		}
	}
}

// createEvents create all the events as specified for each sim resolution interval
func (e *eventSim) createEvents() (events []entity.Event, err error) {
	nbEventsToCreate := e.calculateEventCount()
	for i := 0; i < nbEventsToCreate; i++ {
		var event []byte
		event, err = e.createEvent(e.sourceSpec.EventSpec)
		if err != nil {
			return nil, err
		}
		if e.config.Spec.Ops.LogEventData {
			e.notifier.Notify(entity.NotifyLevelInfo, "event created: %s", string(event))
		}
		events = append(events, entity.Event{Data: event})
	}
	return
}

// calculateEventCount provides the value on how many events to generate for each eventsim trigger
func (e *eventSim) calculateEventCount() int {
	g := e.sourceSpec.EventGeneration
	switch g.Type {
	case EventGenTypeRandom:
		return randInt(g.MinCount, g.MaxCount)
	case EventGenTypeSinusoid:
		return e.createSinusoidEventCount(g)
	default:
		return 1
	}
}

// createSinusoidEventCount returns a number adhering to a sinusoid timeseries, based
// on its config found in EventGeneration parameter.
// To simplify its logic, a pre-requisite is that eg.PeakTime is specified as a timestamp
// earlier than current time.
func (e *eventSim) createSinusoidEventCount(eg EventGeneration) int {
	secondsSincePeakTime := time.Now().Unix() - e.peakTime.Unix()
	angle := float64(secondsSincePeakTime) / float64(eg.PeriodSeconds) * 2 * math.Pi
	value := (math.Cos(angle)+1)/2*(float64(eg.MaxCount)-float64(eg.MinCount)) + float64(eg.MinCount)
	return int(math.Round(value))
}

// createEvent generates a single random event based on the event spec
func (e *eventSim) createEvent(eventSpec EventSpec) (event []byte, err error) {

	var value any
	for _, fieldSpec := range eventSpec.Fields {

		switch {

		case len(fieldSpec.PredefinedValues) > 0:
			value = e.createFieldValueWithFrequencyFactor(e.frequencyRanges[fieldSpec.Field])

		case fieldSpec.RandomizedValue != nil:
			value, err = e.createRandomizedFieldValue(fieldSpec)
		}

		if err != nil {
			return event, err
		}

		event, err = sjson.SetBytes(event, fieldSpec.Field, value)
		if err != nil {
			return event, err
		}
	}
	return event, err
}

// createRandomizedFieldValue handles the fieldSpec option "randomizedValue"
func (e *eventSim) createRandomizedFieldValue(f FieldSpec) (value any, err error) {

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v due to invalid randomizedValue spec: %+v", r, f.RandomizedValue)
		}
	}()
	v := f.RandomizedValue

	switch v.Type {

	case "int", "integer":
		value = randInt(int(v.Min), int(v.Max))

	case "float":
		value = e.generateRandomFloat(v)

	case "string":
		value = e.generateRandomString(v)

	case "bool", "boolean":
		value = randBool()

	case "isoTimestampMilliseconds":
		value = randIsoMillis(v.JitterMilliseconds)

	case "isoTimestampMicroseconds":
		value = randIsoMicros(v.JitterMilliseconds)

	case "uuid":
		value = uuid.New().String()

	default:
		err = fmt.Errorf("unsupported type for randomized values: %s", v.Type)
	}

	return
}

func (e *eventSim) generateRandomFloat(v *RandomizedValue) json.Number {
	if v.MaxFractionDigits == 0 {
		v.MaxFractionDigits = DefaultMaxFractionDigits
	}
	return randFloat(v.Min, v.Max, v.MaxFractionDigits)
}

func (e *eventSim) generateRandomString(v *RandomizedValue) string {
	charset, ok := e.charsets[v.Charset]
	if !ok {
		charset = DefaultCharset
	}
	return randString(int(v.Min), int(v.Max), charset)
}

// randInt creates a random int between min and max (including max)
func randInt(min, max int) int {
	return rand.Intn(max+1-min) + min
}

// randFloat is custom made for this JSON event generation purpose. Formatting and returning
// as a json.Number is one of the few ways possible to efficiently keep the required number
// of fraction digits without introducing standard but unwanted floating point inaccuracy digits,
// when injecting the float via sjson.SetBytes().
func randFloat(min, max float64, fractionDigits int) json.Number {
	randFloat := rand.Float64() * max
	if randFloat < min {
		randFloat = min
	}
	return json.Number(fmt.Sprintf("%.*f", fractionDigits, randFloat))
}

func randString(min, max int, charset []rune) string {
	charsetSize := len(charset)
	strLength := randInt(min, max)
	var sb strings.Builder

	for i := 0; i < strLength; i++ {
		ch := charset[rand.Intn(charsetSize)]
		sb.WriteRune(ch)
	}
	s := sb.String()
	return s
}

func randBool() bool {
	return rand.Intn(2) == 0 // there are slightly faster ways, but it's good enough
}

func randIsoMillis(jitterMillis int) string {
	return currentTimeWithJitter(jitterMillis).Format(TimestampLayoutIsoMillis)
}

func randIsoMicros(jitterMillis int) string {
	return currentTimeWithJitter(jitterMillis).Format(TimestampLayoutIsoMicros)
}

func currentTimeWithJitter(jitterMillis int) time.Time {
	if jitterMillis == 0 {
		return time.Now().UTC()
	}
	jitterNano := time.Duration(jitterMillis) * time.Millisecond
	delta := rand.Int63n(2 * int64(jitterNano))
	delta -= int64(jitterNano)
	deltaDuration := time.Duration(delta)
	return time.Now().UTC().Add(deltaDuration)
}

// FieldFrequencyRange is used for internal conversion of any provided dimensions that
// should be randomized with a given distribution.
type FieldFrequencyRange struct {
	Start int
	End   int
	Max   int
	Value any
}

// createFrequencyRanges prepares the data to be used for generating dimension values
// with the requested distribution/value frequency.
func createFrequencyRanges(fields []FieldSpec) map[string][]FieldFrequencyRange {

	ranges := make(map[string][]FieldFrequencyRange)
	for _, fieldSpec := range fields {

		var (
			fieldRange              []FieldFrequencyRange
			adjustedFieldSpecValues []PredefinedValue
			freqFactorSum           int
		)
		for _, value := range fieldSpec.PredefinedValues {
			if value.FrequencyFactor == 0 {
				value.FrequencyFactor = 1
			}
			freqFactorSum += value.FrequencyFactor
			adjustedFieldSpecValues = append(adjustedFieldSpecValues, value)
		}

		var (
			index       int
			rangedField FieldFrequencyRange
		)
		for _, value := range adjustedFieldSpecValues {
			rangedField.Start = index
			rangedField.End = index + value.FrequencyFactor
			rangedField.Max = freqFactorSum
			rangedField.Value = value.Value
			index = rangedField.End
			fieldRange = append(fieldRange, rangedField)
		}

		ranges[fieldSpec.Field] = fieldRange
	}
	return ranges
}

func (e *eventSim) createFieldValueWithFrequencyFactor(fields []FieldFrequencyRange) any {

	n := rand.Intn(fields[0].Max)

	for _, field := range fields {
		if n >= field.Start && n < field.End {
			return field.Value
		}
	}

	// If using correct prep logic this should never happen
	e.notifier.Notify(entity.NotifyLevelError, "'Unreachable' section reached (n=%d), fields: %+v\n", n, fields)
	return fields[rand.Intn(len(fields))].Value
}

// Unused Extractor functionality

func (e *eventSim) Extract(ctx context.Context, query entity.ExtractorQuery, result any) (error, bool) {
	return nil, false
}
func (e *eventSim) ExtractFromSink(ctx context.Context, query entity.ExtractorQuery, result *[]*entity.Transformed) (error, bool) {
	return nil, false
}
func (e *eventSim) SendToSource(ctx context.Context, event any) (string, error) {
	return "", nil
}

// A context aware sleep func returning true if proper timeout after sleep and false if ctx canceled
func sleepCtx(ctx context.Context, delay time.Duration) bool {
	select {
	case <-time.After(delay):
		return true
	case <-ctx.Done():
		return false
	}
}
