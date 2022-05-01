package xbigquery

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

// Geist's BigQuery Loader uses GCP BQ Go client API for its functionality.
// We're decoupling this API here on consumer side for full unit test capabilities.
// Wrapping is required due to GCP Go client library design constraints.

type BigQueryClient interface {
	GetDatasetMetadata(ctx context.Context, dataset *bigquery.Dataset) (*bigquery.DatasetMetadata, DatasetTableStatus, error)
	CreateDatasetRef(datasetId string) *bigquery.Dataset
	CreateDataset(ctx context.Context, id string, md *bigquery.DatasetMetadata) error
	GetTableMetadata(ctx context.Context, table *bigquery.Table) (*bigquery.TableMetadata, DatasetTableStatus, error)
	CreateTableRef(datasetId string, tableId string) *bigquery.Table
	CreateTable(ctx context.Context, datasetId string, tableId string, tm *bigquery.TableMetadata) (*bigquery.Table, error)
	GetTableInserter(table *bigquery.Table) BigQueryInserter
	UpdateTable(ctx context.Context, table *bigquery.Table, tm bigquery.TableMetadataToUpdate, etag string) (*bigquery.TableMetadata, error)
}

// Concrete bq wrapper client as returned by NewBigQueryClient
type defaultBigQueryClient struct {
	id     string
	client *bigquery.Client
}

// NewBigQueryClient provides a concrete wrapper client for internal usage by the Loader
func NewBigQueryClient(id string, client *bigquery.Client) *defaultBigQueryClient {
	return &defaultBigQueryClient{
		id:     id,
		client: client,
	}
}

func (b *defaultBigQueryClient) CreateDatasetRef(datasetId string) *bigquery.Dataset {
	return b.client.Dataset(datasetId)
}

func (b *defaultBigQueryClient) CreateDataset(ctx context.Context, id string, md *bigquery.DatasetMetadata) error {

	err := b.client.Dataset(id).Create(ctx, md)

	if err != nil {
		if disregardError(err) {
			bqErr := err.Error()
			e, ok := err.(*googleapi.Error)
			if ok {
				bqErr = fmt.Sprintf("googleapi code: %d, message: %s, details: %#v, errors: %+v", e.Code, e.Message, e.Details, e.Errors)
			}
			log.Warnf(b.lgprfx()+"disregarding BQ dataset error: %s", bqErr)
			err = nil
		}
	}
	return err
}

func (b *defaultBigQueryClient) CreateTableRef(datasetId string, tableId string) *bigquery.Table {
	return b.client.Dataset(datasetId).Table(tableId)
}

func (b *defaultBigQueryClient) CreateTable(ctx context.Context, datasetId string, tableId string, tm *bigquery.TableMetadata) (*bigquery.Table, error) {

	table := b.client.Dataset(datasetId).Table(tableId)
	err := table.Create(ctx, tm)

	if err != nil {
		if disregardError(err) {
			bqErr := err.Error()
			e, ok := err.(*googleapi.Error)
			if ok {
				bqErr = fmt.Sprintf("googleapi code: %d, message: %s, details: %#v, errors: %+v", e.Code, e.Message, e.Details, e.Errors)
			}
			log.Warnf(b.lgprfx()+"disregarding BQ table error: %v", bqErr)
			err = nil
		} else {
			log.Errorf(b.lgprfx()+"could not create table %+v, metadata: %+v, err: %v", table, tm, err)
		}
	}
	return table, err
}

func (b *defaultBigQueryClient) GetTableInserter(table *bigquery.Table) BigQueryInserter {
	return &defaultBigQueryInserter{
		inserter: table.Inserter(),
	}
}

type DatasetTableStatus int

const (
	Unknown DatasetTableStatus = iota
	Existent
	NonExistent
)

func (b *defaultBigQueryClient) GetTableMetadata(ctx context.Context, table *bigquery.Table) (*bigquery.TableMetadata, DatasetTableStatus, error) {
	var status DatasetTableStatus

	tm, err := table.Metadata(ctx)

	if e, ok := err.(*googleapi.Error); ok && e.Code == http.StatusNotFound {
		status = NonExistent
	} else if tm != nil && err == nil {
		status = Existent
	}

	return tm, status, err
}

func (b *defaultBigQueryClient) GetDatasetMetadata(ctx context.Context, dataset *bigquery.Dataset) (*bigquery.DatasetMetadata, DatasetTableStatus, error) {
	var status DatasetTableStatus

	tm, err := dataset.Metadata(ctx)

	if e, ok := err.(*googleapi.Error); ok && e.Code == http.StatusNotFound {
		status = NonExistent
	} else if tm != nil && err == nil {
		status = Existent
	}

	return tm, status, err
}

func (b *defaultBigQueryClient) UpdateTable(
	ctx context.Context,
	table *bigquery.Table,
	tm bigquery.TableMetadataToUpdate,
	etag string) (*bigquery.TableMetadata, error) {

	tmOut, err := table.Update(ctx, tm, etag)

	if err != nil {
		if disregardError(err) {
			bqErr := err.Error()
			e, ok := err.(*googleapi.Error)
			if ok {
				bqErr = fmt.Sprintf("googleapi code: %d, message: %s, details: %#v, errors: %+v", e.Code, e.Message, e.Details, e.Errors)
			}
			log.Warnf(b.lgprfx()+"disregarding BQ table update error: %v, returned metadata: %+v", bqErr, tmOut)
			err = nil
			tmOut, err = table.Metadata(ctx)
		}
	}

	return tmOut, err
}

func (b *defaultBigQueryClient) lgprfx() string {
	return "[xbigquery.client:" + b.id + "] "
}

// No good granular way to properly get real error codes from bq client, to detect these "non-errors" (in BQ loader scenarios).
// Need to parse error string -.-
func disregardError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "already exists")
}

//
// BigQueryInserter
//

type BigQueryInserter interface {
	Put(ctx context.Context, src any) error
}

type defaultBigQueryInserter struct {
	inserter *bigquery.Inserter
}

func (i *defaultBigQueryInserter) Put(ctx context.Context, src any) error {
	return i.inserter.Put(ctx, src)
}

func isNil(v any) bool {
	return v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil())
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
