package model

type QueryType int

const (
	Unknown QueryType = iota
	KeyValue
	CompositeKeyValue
	All
)

type ExtractorQuery struct {
	Type         QueryType
	Key          string
	CompositeKey []KeyValueFilter
}

type KeyValueFilter struct {
	Key   string
	Value string
}
