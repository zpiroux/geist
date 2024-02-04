package eventsim

import "fmt"

// GenerateFieldsFromSetOfStringsSpec processes the provided field spec and returns
// a new field spec containing the generated set of predefined values as was required
// in the setOfFields part of the original input spec.
func GenerateFieldsFromSetOfStringsSpec(fields []FieldSpec) (generatedFields []FieldSpec) {
	for _, field := range fields {
		if field.SetOfStrings != nil {
			newField := fieldFromSetOfStrings(field)
			generatedFields = append(generatedFields, newField)
		}
	}
	return
}

// fieldFromSetOfStrings assumes inField.SetOfStrings is not nil
func fieldFromSetOfStrings(inField FieldSpec) (outField FieldSpec) {
	outField.Field = inField.Field
	ssSpec := inField.SetOfStrings

	var predefValues []PredefinedValue
	for i := 0; i < ssSpec.Amount; i++ {
		var predefValue PredefinedValue
		value := fmt.Sprintf("%s%d", ssSpec.Prefix, i+1)
		if contains(value, ssSpec.ExcludeValues) {
			continue
		}
		predefValue.Value = value
		predefValue.FrequencyFactor = freqFactor(ssSpec)
		predefValues = append(predefValues, predefValue)
	}
	outField.PredefinedValues = predefValues
	return
}

func freqFactor(spec *SetOfStrings) int {
	factor := 1
	switch {
	case spec.FrequencyMax < 1:
	case spec.FrequencyMin < 1:
	case spec.FrequencyMax <= spec.FrequencyMin:
	default:
		factor = randInt(spec.FrequencyMin, spec.FrequencyMax)
	}
	return factor
}

func contains(str string, strs []string) bool {
	for _, s := range strs {
		if str == s {
			return true
		}
	}
	return false
}
