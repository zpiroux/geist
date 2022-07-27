package entity

import (
	"encoding/json"
	"fmt"
	"time"
)

// RowItem or an array of row items can be used as map value in a Transformed output from
// for example the Extractor.ExtractFromSink() function, e.g. providing data from a BigTable row
type RowItem struct {
	Column    string    `json:"column"`
	Timestamp time.Time `json:"timestamp"`
	Value     any       `json:"value"`
}

// TransformedItemMap is the type used for transforms creating a map of items to be stored in the output map.
// One example is the ExtractItemsFromArray transform, which extracts JSON array items into such a map
// and stores that map inside the output Transformed map array.
type TransformedItemMap map[string]any

const (
	TransformedKeyKey   = "key"
	TransformedValueKey = "value"
)

type Transformed struct {
	Data map[string]any `json:"data"`
}

func NewTransformed() *Transformed {
	return &Transformed{
		Data: make(map[string]any),
	}
}

func (t *Transformed) String() string {

	var strOut = "{ "
	j := 0
	for key, value := range t.Data {
		if j > 0 {
			strOut += ", "
		}
		j++
		var str string
		switch value := value.(type) {
		case bool:
			str = fmt.Sprintf("%v (bool)", value)
		case int:
			str = fmt.Sprintf("%d (int)", value)
		case int64:
			str = fmt.Sprintf("%d (int64)", value)
		case float64:
			str = fmt.Sprintf("%v (float64)", value)
		case string:
			str = fmt.Sprintf("%s (string)", value)
		case []byte:
			str = fmt.Sprintf("%s ([]byte)", string(value))
		case time.Time:
			str = fmt.Sprintf("%s (time.Time)", value.Format("2006-01-02T15:04:05.000Z"))
		case []*RowItem:
			str = "[ "
			for i, item := range value {
				if i > 0 {
					str += "], "
				}
				jsonBytes, _ := json.Marshal(item)
				str += string(jsonBytes)
			}
			str += " ]"
		case map[string]any:
			str = fmt.Sprintf("%v (map[string]any)", value)
		default:
			str = fmt.Sprintf("ERROR: unhandled type (%T) in Transformed.String()", value)
		}
		strOut += fmt.Sprintf("\"%s\": \"%s\"", key, str)
	}
	return strOut + " }"
}
