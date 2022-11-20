package entity

import (
	"fmt"
	"regexp"
	"strings"
)

// validate is used during spec creation with entity.NewSpec() if Spec.Transform.Regexp
// is specified in the stream spec.
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
