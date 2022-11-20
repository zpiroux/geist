package entity

import (
	"fmt"
	"regexp"
	"strings"
)

// Regexp specifies an optional transformation type for use in the stream spec.
// It transforms a string into a JSON based on the groupings in the regular expression.
// Minimum one grouping needs to be made. The resulting output from the transformation
// is found with the key "regexppayload".
// An example use case is when having incoming events on a Kafka topic with certain fields
// containing plain log text strings, from which certain parts should be extracted into
// fields for downstream ingestion into the sink on a structured format.
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

type TimeConv struct {
	// Field where the data is located and should be converted.
	Field string `json:"field,omitempty"`

	// Input format of date to be converted. Mandatory.
	InputFormat string `json:"inputFormat,omitempty"`

	// Output format of date, if omitted, ISO-8601 is used.
	OutputFormat string `json:"outputFormat,omitempty"`
}

// Validate returns nil if the Regexp transform spec is valid, or an error otherwise.
func (r *Regexp) Validate() (err error) {
	if r.Expression == "" {
		return fmt.Errorf("no RegExp is specified")
	}

	_, err = regexp.Compile(r.Expression)
	if err != nil {
		return fmt.Errorf("error during RegExp compile: %v", err.Error())
	}

	groups := r.CollectGroups(r.Expression)
	if len(groups) < 1 {
		return fmt.Errorf("no groupings where found in regular expression %s", r.Expression)
	}

	if r.TimeConversion != nil {
		if len(r.TimeConversion.Field) < 1 {
			return fmt.Errorf("regexp.timeConversion.field must be set")
		}
		if len(r.TimeConversion.InputFormat) < 1 {
			return fmt.Errorf("regexp.timeConversion.inputFormat must be set")
		}
	}
	return err
}

// TODO: Shorten, but not important now since there is no performance impact.
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
