package xbigtable

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigtable"
	"github.com/zpiroux/geist/internal/pkg/model"
)

type QueryType int

const (
	InvalidQueryType QueryType = iota
	KeyValue
	LatestN
	All
)

type Query struct {
	Type    QueryType
	Table   string
	RowKey  string
	LatestN int
}

type Extractor struct {
	spec         model.Spec
	client       BigTableClient
	adminClient  BigTableAdminClient
	openedTables map[string]BigTableTable
}

func NewExtractor(
	spec model.Spec,
	client BigTableClient,
	adminClient BigTableAdminClient) (*Extractor, error) {

	if isNil(client) || isNil(adminClient) {
		return nil, errors.New("invalid arguments, clients cannot be nil")
	}
	var e = Extractor{
		spec:        spec,
		client:      client,
		adminClient: adminClient,
	}

	e.openedTables = make(map[string]BigTableTable)

	return &e, nil
}

func (e *Extractor) ExtractFromSink(ctx context.Context, query model.ExtractorQuery, result *[]*model.Transformed) (error, bool) {

	if len(e.spec.Sink.Config.Tables) == 0 {
		return errors.New("need at least one Table specified in Sink config"), false
	}

	return e.extractFromSink(ctx, e.convertQueryToNative(query), result)
}

// For now this does not do much conversion but it's prepared for more complex
// queries with range filters, etc.
func (g *Extractor) convertQueryToNative(query model.ExtractorQuery) Query {

	var nativeQuery Query

	switch query.Type {
	case model.All:
		nativeQuery.Type = All

	case model.KeyValue:
		nativeQuery.Type = KeyValue
		nativeQuery.RowKey = query.Key
		nativeQuery.LatestN = 1
	}

	return nativeQuery
}

func (e *Extractor) extractFromSink(ctx context.Context, query Query, result *[]*model.Transformed) (error, bool) {

	var (
		row bigtable.Row
		err error
	)
	// lazy init of open tables in case the BT loader is creating tables at GEIST startup
	if len(e.openedTables) == 0 {
		if err = e.openTables(ctx); err != nil {
			return err, true
		}
	}

	switch query.Type {
	case KeyValue:

		for _, table := range e.openedTables {
			row, err = table.ReadRow(ctx, query.RowKey, bigtable.RowFilter(bigtable.LatestNFilter(query.LatestN)))
			if err != nil {
				return err, true
			}
			if len(row) > 0 {
				break
			}
		}

		if len(row) == 0 {
			return errors.New("row with key " + query.RowKey + " not found"), false
		}
		// TODO: Keep below comment and make output type configurable - transform recreation or raw output
		// transformed, err, retryable := e.recreateTransformed(&row)
		transformed, err, retryable := e.createTransformedAsRow(query.RowKey, &row)
		if err != nil {
			return err, retryable
		}
		*result = append(*result, transformed)

	default:
		return fmt.Errorf("queryType '%v' not supported", query.Type), false
	}

	return nil, false
}

func (e *Extractor) StreamExtract(
	ctx context.Context,
	reportEvent model.ProcessEventFunc,
	err *error,
	retryable *bool) {

	*err = errors.New("not applicable")
}

func (e *Extractor) SendToSource(ctx context.Context, eventData any) (string, error) {

	return "", errors.New("not applicable")
}

func (e *Extractor) Extract(ctx context.Context, query model.ExtractorQuery, result any) (error, bool) {
	return errors.New("not applicable"), false
}

func (e *Extractor) openTables(ctx context.Context) error {

	// For now only Sink entity is available for BigTable extractor spec
	for _, table := range e.spec.Sink.Config.Tables {

		t := e.client.Open(table.Name)
		if t == nil {
			return fmt.Errorf("could not open table %s", table.Name)
		}
		e.openedTables[table.Name] = t
	}
	return nil
}

// createTransformedAsRow flattens columnFamilies, currently assuming column qualifier names
// are unique across families.
func (e *Extractor) createTransformedAsRow(rowKey string, row *bigtable.Row) (*model.Transformed, error, bool) {

	transformed := model.NewTransformed()
	var rowItems []*model.RowItem

	for _, cols := range *row {
		for _, col := range cols {
			rowItem := &model.RowItem{
				Column:    col.Column,
				Timestamp: col.Timestamp.Time(),
				Value:     string(col.Value),
				//TODO: change from converting to string to look at what was in the transform part of spec
			}
			rowItems = append(rowItems, rowItem)
		}
	}
	transformed.Data[model.TransformedKeyKey] = rowKey
	transformed.Data[model.TransformedValueKey] = rowItems
	return transformed, nil, true
}

func (e *Extractor) setOpenTables(b map[string]BigTableTable) {
	e.openedTables = b
}
