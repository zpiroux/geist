package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/zpiroux/geist/entity"

	"github.com/tidwall/gjson"
)

// Default Transformer implementation (stateless, immutable).
// This is currently the only implementation and supports all transformation types available in a GEIST spec.
// Future custom transformer implementations are easily added through this mechanism.
type Transformer struct {
	spec   *entity.Spec   // The full ETL Stream spec
	regexp *regexp.Regexp // Regexp object
	groups []string       // Contains capture groups in RegExp
}

func NewTransformer(spec *entity.Spec) *Transformer {
	var t Transformer

	t.spec = spec
	if t.spec.Transform.Regexp != nil {
		t.regexp, _ = regexp.Compile(t.spec.Transform.Regexp.Expression)
		t.groups = t.spec.Transform.Regexp.CollectGroups(t.spec.Transform.Regexp.Expression)
	}

	return &t
}

// Transform returns a key-value map, based on input event data and the transformation rules in the Spec,
// where keys are "id" fields from Transform spec, and values are the transformation results.
// The output from Transform() can contain multiple new events, in case of applied
// event-split transformations.
// If Transform() succeeded, but the transformation resulted in no output, e.g. for non-applicable incoming
// events, the return values are nil, nil (i.e., not regarded as an error).
func (t *Transformer) Transform(
	ctx context.Context,
	event []byte,
	retryable *bool) ([]*entity.Transformed, error) {

	var transformed []*entity.Transformed
	*retryable = false

	if len(t.spec.Transform.ExcludeEventsWith) > 0 {
		if t.shouldExclude(event, &transformed) {
			return nil, nil
		}
	}

	if len(t.spec.Transform.ExtractFields) > 0 {
		if err := t.extractFieldsTransform(event, &transformed); err != nil {
			return nil, err
		}
	}

	if len(t.spec.Transform.ExtractItemsFromArray) > 0 {
		if err := t.extractItemsFromArrayTransform(event, &transformed); err != nil {
			return nil, err
		}
	}

	if t.spec.Transform.Regexp != nil {
		if err := t.regexpTransform(event, &transformed); err != nil {
			return nil, err
		}
	}

	return transformed, nil
}

func (t *Transformer) shouldExclude(event []byte, transformed *[]*entity.Transformed) (exclude bool) {

	for _, filter := range t.spec.Transform.ExcludeEventsWith {

		valueToCheck := gjson.GetBytes(event, filter.Key)
		if !valueToCheck.Exists() {
			if excludeIfEmpty(filter.ValueIsEmpty) {
				return true
			}
			continue
		}
		value := valueToCheck.String()

		if len(filter.Values) > 0 {
			exclude = excludeIfInBlacklist(value, filter.Values)
		} else if len(filter.ValuesNotIn) > 0 {
			exclude = excludeIfNotInWhitelist(value, filter.ValuesNotIn)
		}
		if exclude {
			break
		}
	}
	return
}

func excludeIfEmpty(filterValueIsEmpty *bool) bool {
	if filterValueIsEmpty != nil {
		if *filterValueIsEmpty {
			return true
		}
	}
	return false
}

func excludeIfInBlacklist(value string, filterValues []string) bool {
	for _, excludeIfValue := range filterValues {
		if value == excludeIfValue {
			return true
		}
	}
	return false
}

func excludeIfNotInWhitelist(value string, filterValues []string) bool {
	for _, includeIfValue := range filterValues {
		if value == includeIfValue {
			return false
		}
	}
	return true
}

func (t *Transformer) extractFieldsTransform(event []byte, transformed *[]*entity.Transformed) error {
	for _, fieldExtraction := range t.spec.Transform.ExtractFields {
		if applicableEvent(fieldExtraction.ForEventsWith, event) {
			result, err := extractFields(fieldExtraction, event)
			if err != nil {
				return err
			}
			*transformed = append(*transformed, result)
		}
	}
	return nil
}

func (t *Transformer) extractItemsFromArrayTransform(event []byte, transformed *[]*entity.Transformed) error {

	for _, itemExtraction := range t.spec.Transform.ExtractItemsFromArray {
		if applicableEvent(itemExtraction.ForEventsWith, event) {
			result, err := extractItemsFromArray(itemExtraction, event)
			if err != nil {
				return err
			}
			*transformed = append(*transformed, result)
		}
	}
	return nil
}

// regexpTransform assumes t.spec.Transform.Regexp is not nil
func (t *Transformer) regexpTransform(event []byte, transformed *[]*entity.Transformed) error {
	// TODO: Improve whole regexp and make it possible to chain
	var (
		transform *entity.Transformed
		err       error
	)

	transform, event, err = t.preProcessRegExp(event, transformed)
	if err != nil {
		return err
	}

	result, err := applyRegExp(t.regexp, t.spec.Transform.Regexp, event, transform, t.groups)
	if err != nil {
		return err
	}
	if len(*transformed) < 1 {
		*transformed = append(*transformed, result)
	}
	return nil
}

func (t *Transformer) preProcessRegExp(event []byte, transformed *[]*entity.Transformed) (*entity.Transformed, []byte, error) {
	var (
		transform *entity.Transformed
		err       error
	)

	if len(t.spec.Transform.Regexp.Field) > 0 {
		if len(*transformed) == 0 {
			return transform, event, fmt.Errorf("wanted field: %s was not extracted", t.spec.Transform.Regexp.Field)
		}
		for _, td := range *transformed {
			fe, ok := td.Data[t.spec.Transform.Regexp.Field]
			if !ok {
				continue
			}
			if event, err = getBytes(fe); err != nil {
				return transform, event, fmt.Errorf("could not cast field %v into byte", fe)
			}
			transform = td
			if !t.spec.Transform.Regexp.KeepField {
				delete(td.Data, t.spec.Transform.Regexp.Field)
			}
			break
		}
	}
	return transform, event, err
}

func applyRegExp(regex *regexp.Regexp, regexpSpec *entity.Regexp, event []byte, input *entity.Transformed, groups []string) (res *entity.Transformed, err error) {
	// Panics if its a non-match for RegExp. Improves perf with 2x to not do explicit checking so will just fail-fast instead.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf(fmt.Sprintf("Received non-matching event for RegExp in transform. Event: %s", string(event)))
		}
	}()

	// Only instantiate new if not working on pointer ref.
	if input == nil {
		input = entity.NewTransformed()
	}

	parsed := regex.FindStringSubmatch(string(event))[1:]
	d := make(map[string]any, len(parsed))
	for i, n := range parsed {
		d[groups[i]] = n
	}

	if regexpSpec.TimeConversion != nil {
		date, ok := d[regexpSpec.TimeConversion.Field].(string)
		if !ok {
			return nil, fmt.Errorf("could not cast timeconversion field: %s to string", regexpSpec.TimeConversion.Field)
		}
		formatDate, err := timeConv(regexpSpec.TimeConversion, date)
		if err != nil {
			return nil, fmt.Errorf("error parsing date for event: %s", string(event))
		}
		d[regexpSpec.TimeConversion.Field] = formatDate
	}

	// TODO: Change this to be configurable
	db, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	input.Data["regexppayload"] = db

	return input, err
}

// Checks all filter rules in spec to see if incoming event should be processed or not.
// Currently only AND type of key-value filters are supported in ForEventsWith
func applicableEvent(eventFilter []entity.ForEventsWith, event []byte) bool {

	if len(eventFilter) == 0 {
		return true
	}

	applicable := false
	for _, keyFilter := range eventFilter {

		value := gjson.GetBytes(event, keyFilter.Key)
		if value.Exists() {
			if value.String() == keyFilter.Value {
				applicable = true
			} else {
				applicable = false
			}
		} else {
			applicable = false
		}
	}

	return applicable
}

func extractFields(fieldExtraction entity.ExtractFields, event []byte) (*entity.Transformed, error) {

	var err error

	transformed := entity.NewTransformed()

	for _, field := range fieldExtraction.Fields {
		if len(field.JsonPath) == 0 {
			transformed.Data[field.Id] = transformRawEvent(field.Type, event)
			continue
		}
		value := gjson.GetBytes(event, field.JsonPath)

		switch field.Type {
		case "bool", "boolean":
			transformed.Data[field.Id] = value.Bool()
		case "int", "integer":
			transformed.Data[field.Id] = value.Int()
		case "float":
			transformed.Data[field.Id] = value.Float()
		case "isoTimestamp":
			transformed.Data[field.Id] = value.Time()
		case "unixTimestamp":
			transformed.Data[field.Id] = convertFromMillisToGoTime(value.Int())
		case "userAgent":
			transformed.Data[field.Id], err = convertFromUAStringToUAJSON(value.String())
		default:
			transformed.Data[field.Id] = value.String()
		}
	}

	return transformed, err
}

// extractItemsFromArray extracts JSON objects inside a JSON array and stores those in a map which is
// itself stored in the Transformed output map with key/id set to ExtractItemsFromArray.Id from the stream spec.
func extractItemsFromArray(spec entity.ExtractItemsFromArray, event []byte) (*entity.Transformed, error) {

	transformed := entity.NewTransformed()
	result := gjson.GetBytes(event, spec.Items.JsonPathToArray)
	result.ForEach(func(key, value gjson.Result) bool {
		var idStrings []string
		for _, field := range spec.Items.IdFromItemFields.Fields {
			fieldValue := gjson.Get(value.Raw, field)
			idStrings = append(idStrings, fieldValue.Str)
		}
		id := strings.Join(idStrings, spec.Items.IdFromItemFields.Delimiter)

		if id != "" {
			transformed.Data[id] = value.Raw
		}
		return true
	})

	outTransformed := entity.NewTransformed()
	outTransformed.Data[spec.Id] = transformed.Data
	return outTransformed, nil
}

func transformRawEvent(fieldType string, event []byte) any {
	switch fieldType {
	case "string":
		return string(event)
	default:
		return event
	}
}

func convertFromMillisToGoTime(millis int64) time.Time {
	return time.Unix(0, millis*1000000).UTC()
}

func getBytes(v any) ([]byte, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("could not cast %v to string", v)
	}

	return []byte(s), nil
}

func timeConv(tcSpec *entity.TimeConv, date string) (string, error) {
	if date == "" {
		return "", fmt.Errorf("field: %s is empty", tcSpec.Field)
	}

	// TODO: Remove this later, quick fix.
	date = strings.ReplaceAll(date, ",", ".")

	t, err := time.Parse(tcSpec.InputFormat, date)
	if err != nil {
		return "", fmt.Errorf("could not parse input format, err: %s", err.Error())
	}
	if len(tcSpec.OutputFormat) < 1 {
		tcSpec.OutputFormat = time.RFC3339
	}
	return t.Format(tcSpec.OutputFormat), nil
}

func convertFromUAStringToUAJSON(uaStr string) (string, error) {
	ua, err := NewUserAgent(uaStr)
	if err != nil {
		return "", err
	}
	return ua.String(), nil
}
