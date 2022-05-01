package xbigquery

import (
	"context"
	"fmt"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/internal/pkg/entity/transform"
	"github.com/zpiroux/geist/internal/pkg/model"
)

const (
	testDirPath  = "../../../../test/"
	testSpecDir  = testDirPath + "specs/"
	testEventDir = testDirPath + "events/"

	fooEventSpec = "kafkasrc-bigquerysink-fooevents.json"
)

var (
	mockDb map[string]string
	tables map[string]*bigquery.TableMetadata
)

func init() {
	mockDb = make(map[string]string)
	tables = make(map[string]*bigquery.TableMetadata)
}

func TestLoader_StreamLoad(t *testing.T) {

	var retryable bool
	ctx := context.Background()

	spec, err := model.NewSpec(genericSourceToBigQuerySinkSpec)
	assert.NoError(t, err)
	assert.NotNil(t, spec)

	loader, err := NewLoader(ctx, spec, "coolLoaderId", &MockBigQueryClient{}, &sync.Mutex{})
	assert.NoError(t, err)
	assert.NotNil(t, loader)

	// Verify table were correctly created according to spec
	md, err := loader.createTableMetadata(ctx, spec.Sink.Config.Tables[0])
	assert.NoError(t, err)
	assert.Equal(t, md, tables["geisttest-gotest_general"])

	transformer := transform.NewTransformer(spec)

	// Simulate two incoming events that should be transformed and inserted to BQ in same multi-row insert
	t1, err := transformer.Transform(context.Background(), testEvent1, &retryable)
	assert.NoError(t, err)
	transformed, err := transformer.Transform(context.Background(), testEvent2, &retryable)
	transformed = append(transformed, t1[0])

	assert.NoError(t, err)
	assert.NotEqual(t, len(transformed), 0)

	// Send events to BQ sink for insertion
	_, err, retryable = loader.StreamLoad(context.Background(), transformed)
	assert.NoError(t, err)

	// Verify events are processed and inserted according to schema in GEIST stream spec
	assert.Equal(t, string(testEvent1), mockDb["coolEventName"])
	assert.Equal(t, "testEvent1eventId", mockDb["coolEventName-insertId"])
	assert.Equal(t, string(testEvent2), mockDb["niceEventName"])
	assert.Equal(t, "testEvent2eventId", mockDb["niceEventName-insertId"])
}

func TestSpecificStreams(t *testing.T) {
	ctx := context.Background()

	// Test prod-like event ingestion spec
	spec, err := getTestSpec(fooEventSpec)
	assert.NoError(t, err)
	assert.NotNil(t, spec)

	loader, err := NewLoader(ctx, spec, "coolLoaderId", &MockBigQueryClient{}, &sync.Mutex{})
	assert.NoError(t, err)
	assert.NotNil(t, loader)

	md, err := loader.createTableMetadata(ctx, spec.Sink.Config.Tables[0])
	assert.NoError(t, err)
	assert.Equal(t, md, tables["geisttest-fooevents_v1"])
	assert.Equal(t, []string{"customerId"}, tables["geisttest-fooevents_v1"].Clustering.Fields)
	assert.Equal(t, bigquery.TimePartitioningType("DAY"), tables["geisttest-fooevents_v1"].TimePartitioning.Type)
}

func TestDynamicColumnCreation(t *testing.T) {

	var retryable bool
	ctx := context.Background()

	spec, err := model.NewSpec(streamSpecWithDynamicColumns)
	assert.NoError(t, err)
	assert.NotNil(t, spec)

	loader, err := NewLoader(ctx, spec, "coolLoaderId", &MockBigQueryClient{}, &sync.Mutex{})
	assert.NoError(t, err)
	assert.NotNil(t, loader)

	// Verify table were correctly created according to spec
	md, err := loader.createTableMetadata(ctx, spec.Sink.Config.Tables[0])
	assert.NoError(t, err)
	assert.Equal(t, md, tables["geisttest-gotest_dynamiccolumns"])

	transformer := transform.NewTransformer(spec)

	// Simulate one incoming event which should trigger a table update (new column created)
	t1, err := transformer.Transform(context.Background(), testEvent3, &retryable)
	assert.NoError(t, err)

	// Table should only have one column before event is inserted
	tm := tables["geisttest-gotest_dynamiccolumns"]
	assert.Equal(t, 1, len(tm.Schema))
	assert.Equal(t, "dateIngested", tm.Schema[0].Name)

	// Send events to BQ sink for insertion
	_, err, retryable = loader.StreamLoad(context.Background(), t1)
	assert.NoError(t, err)

	// Table should have two columns after event is inserted
	tm = tables["geisttest-gotest_dynamiccolumns"]
	assert.Equal(t, 2, len(tm.Schema))
	assert.Equal(t, "COOL_THING_HAPPENED", tm.Schema[1].Name)

	// Same event should try to create new columns
	_, err, retryable = loader.StreamLoad(context.Background(), t1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tm.Schema))
}

func TestColumnCloning(t *testing.T) {

	var retryable bool
	ctx := context.Background()

	spec, err := model.NewSpec(streamSpecWithColumnCloneFeature)
	assert.NoError(t, err)
	assert.NotNil(t, spec)

	loader, err := NewLoader(ctx, spec, "coolLoaderId", &MockBigQueryClient{}, &sync.Mutex{})
	assert.NoError(t, err)
	assert.NotNil(t, loader)

	// Verify table were correctly created according to spec
	md, err := loader.createTableMetadata(ctx, spec.Sink.Config.Tables[0])
	assert.NoError(t, err)
	assert.Equal(t, md, tables["geisttest-gotest_columncloning"])

	tm := tables["geisttest-gotest_columncloning"]
	assert.Equal(t, 3, len(tm.Schema))
	assert.Equal(t, "dateIngested", tm.Schema[0].Name)
	assert.Equal(t, "col1", tm.Schema[1].Name)
	assert.Equal(t, "col2", tm.Schema[2].Name)

	transformer := transform.NewTransformer(spec)

	col1Event := []byte(`{"name": "col1", "someData": "x"}`)
	col2Event := []byte(`{"name": "col2", "someData": "y"}`)
	t1, err := transformer.Transform(context.Background(), col1Event, &retryable)
	assert.NoError(t, err)
	t2, err := transformer.Transform(context.Background(), col2Event, &retryable)
	assert.NoError(t, err)
	t2 = append(t2, t1[0])
	fmt.Printf("t2: %s\n", t2)

	_, err, retryable = loader.StreamLoad(context.Background(), t2)
	assert.NoError(t, err)

	// Verify events are processed and inserted according to schema in GEIST stream spec
	assert.Equal(t, string(col1Event), mockDb["col1"])
	assert.Equal(t, string(col2Event), mockDb["col2"])
}

func TestDataValidation(t *testing.T) {

	col := model.Column{
		Name: "col1",
		Type: "STRING",
		Mode: "NULLABLE",
	}
	err := validateData(col, "mycoolstring")
	assert.NoError(t, err)
	err = validateData(col, 666)
	assert.Error(t, err)
	err = validateData(col, nil)
	assert.NoError(t, err)

	col = model.Column{
		Name: "col1",
		Type: "STRING",
		Mode: "REQUIRED",
	}
	err = validateData(col, 666)
	assert.Error(t, err)
	err = validateData(col, nil)
	assert.Error(t, err)

	col = model.Column{
		Name: "col1",
		Type: "TIMESTAMP",
		Mode: "REQUIRED",
	}
	err = validateData(col, time.Now())
	assert.NoError(t, err)
	err = validateData(col, "mycooltimestamp")
	assert.Error(t, err)
	emptyTimestamp := time.Time{}
	err = validateData(col, emptyTimestamp)
	assert.Error(t, err)

	col = model.Column{
		Name: "col1",
		Type: "NUMERIC",
		Mode: "NULLABLE",
	}
	err = validateData(col, int(0))
	assert.NoError(t, err)
	err = validateData(col, int64(2))
	assert.NoError(t, err)
	err = validateData(col, float64(7))
	assert.NoError(t, err)
	err = validateData(col, nil)
	assert.NoError(t, err)
	err = validateData(col, "666")
	assert.Error(t, err)

	col = model.Column{
		Name: "col1",
		Type: "BYTES",
		Mode: "NULLABLE",
	}
	err = validateData(col, []byte("kardemummabulle"))
	assert.NoError(t, err)
	err = validateData(col, "kardemummabulle")
	assert.Error(t, err)

	col = model.Column{
		Name: "col1",
		Type: "RECORD",
		Mode: "REQUIRED",
	}
	err = validateData(col, "foo")
	assert.NoError(t, err)
	err = validateData(col, nil)
	assert.Error(t, err)

	col = model.Column{
		Name: "col1",
		Type: "REPEATED",
		Mode: "REQUIRED",
	}
	err = validateData(col, "foo")
	assert.NoError(t, err)
	err = validateData(col, nil)
	assert.Error(t, err)
}

func getTestSpec(specFile string) (*model.Spec, error) {
	fileBytes, err := ioutil.ReadFile(testSpecDir + specFile)
	if err != nil {
		return nil, err
	}
	spec, err := model.NewSpec(fileBytes)
	if err != nil {
		return nil, err
	}
	if err = spec.Validate(); err != nil {
		return nil, err
	}
	return spec, err
}

type MockBigQueryClient struct{}

func (b *MockBigQueryClient) GetDatasetMetadata(ctx context.Context, dataset *bigquery.Dataset) (*bigquery.DatasetMetadata, DatasetTableStatus, error) {
	return &bigquery.DatasetMetadata{}, Unknown, nil
}

func (b *MockBigQueryClient) CreateDatasetRef(datasetId string) *bigquery.Dataset {
	return &bigquery.Dataset{DatasetID: datasetId}
}

func (b *MockBigQueryClient) CreateDataset(ctx context.Context, id string, md *bigquery.DatasetMetadata) error {
	return nil
}

func (b *MockBigQueryClient) CreateTableRef(datasetId string, tableId string) *bigquery.Table {
	return &bigquery.Table{DatasetID: datasetId, TableID: tableId}
}

func (b *MockBigQueryClient) CreateTable(ctx context.Context, datasetId string, tableId string, tm *bigquery.TableMetadata) (*bigquery.Table, error) {

	tables[datasetId+"-"+tableId] = tm

	return &bigquery.Table{DatasetID: datasetId, TableID: tableId}, nil
}

func (b *MockBigQueryClient) GetTableInserter(table *bigquery.Table) BigQueryInserter {
	return &MockBigQueryInserter{}
}

func (b *MockBigQueryClient) GetTableMetadata(ctx context.Context, table *bigquery.Table) (*bigquery.TableMetadata, DatasetTableStatus, error) {

	tm, ok := tables[table.DatasetID+"-"+table.TableID]
	if !ok {
		return nil, NonExistent, fmt.Errorf("table not found, table: %+v", table)
	}
	return tm, Unknown, nil
}

func (b *MockBigQueryClient) UpdateTable(
	ctx context.Context,
	table *bigquery.Table,
	tmToUpdate bigquery.TableMetadataToUpdate,
	etag string) (*bigquery.TableMetadata, error) {

	fieldStr := ""
	for _, field := range tmToUpdate.Schema {
		fieldStr = fieldStr + fmt.Sprintf("column: %+v, \n", field)
	}

	fmt.Printf("UpdateTable() - tableMetadataToUpdate: %+v\nColumns: %s", tmToUpdate, fieldStr)

	tm := tables[table.DatasetID+"-"+table.TableID]
	tm.Schema = tmToUpdate.Schema
	tables[table.DatasetID+"-"+table.TableID] = tm
	return tm, nil
}

func (b *MockBigQueryClient) Close() error {
	return nil
}

type MockBigQueryInserter struct{}

func (i *MockBigQueryInserter) Put(ctx context.Context, src any) error {

	rows := src.([]*Row)

	for _, row := range rows {
		fmt.Printf("inserting row: %+v\n", row)

		rowItems, insertId, err := row.Save()
		if err != nil {
			return err
		}
		key, ok := rowItems["eventName"]
		if ok {
			mockDb[key.(string)] = rowItems["eventData"].(string)
			mockDb[key.(string)+"-insertId"] = insertId
		} else {
			for k, v := range rowItems {
				if strValue, ok := v.(string); ok { // simplify
					mockDb[k] = strValue
				}
			}
		}
	}
	return nil
}

var testEvent1 = []byte(`
{
   "name": "coolEventName",
   "eventId": "testEvent1eventId",
   "lotsOfData": {
      "field1": "foo",
      "field2": "bar"
	}
}
`)

var testEvent2 = []byte(`
{
   "name": "niceEventName",
   "eventId": "testEvent2eventId",
   "lotsOfData": {
      "field1": "foo2",
      "field2": "bar2"
	}
}
`)

var testEvent3 = []byte(`
{
   "name": "COOL_THING_HAPPENED",
   "eventId": "testEvent3eventId",
   "lotsOfData": {
      "field1": "foo3",
      "field2": "bar3"
	}
}
`)

var genericSourceToBigQuerySinkSpec = []byte(`
{
   "namespace": "geist",
   "streamIdSuffix": "xtobigquery",
   "description": "Generic spec for any source storing raw events in a simple table",
   "version": 1,
   "ops": {
      "logEventData": true
   },
   "source": {
      "type": "kafka"
   },
   "transform": {
      "extractFields": [
         {
            "fields": [
               {
                  "id": "eventNameId",
                  "jsonPath": "name"
               },
               {
                  "id": "eventId",
                  "jsonPath": "eventId"
               },
               {
                  "id": "rawEventId",
                  "type": "string"
               }
            ]
         }
      ]
   },
   "sink": {
      "type": "bigquery",
      "config": {
         "tables": [
            {
               "name": "gotest_general",
               "dataset": "geisttest",
               "insertIdFromId": "eventId",
               "columns": [
                  {
                     "name": "eventName",
                     "mode": "REQUIRED",
                     "type": "STRING",
                     "description": "name of the event",
                     "valueFromId": "eventNameId"
                  },
                  {
                     "name": "eventData",
                     "mode": "NULLABLE",
                     "type": "STRING",
                     "description": "raw event data",
                     "valueFromId": "rawEventId"
                  },
                  {
                     "name": "dateIngested",
                     "mode": "NULLABLE",
                     "type": "TIMESTAMP",
                     "description": "ingestion timestamp",
                     "valueFromId": "@GeistIngestionTime"
                  }
               ]
            }
         ]
      }
   }
}
`)

var streamSpecWithDynamicColumns = []byte(`
{
   "namespace": "geist",
   "streamIdSuffix": "xtobigquery_dynamiccolumns",
   "description": "Spec where new BQ columns should be generated on the fly based on event data",
   "version": 1,
   "ops": {
      "logEventData": true
   },
   "source": {
      "type": "pubsub"
   },
   "transform": {
      "extractFields": [
         {
            "fields": [
               {
                  "id": "eventNameId",
                  "jsonPath": "name"
               },
               {
                  "id": "eventId",
                  "jsonPath": "eventId"
               },
               {
                  "id": "rawEventId",
                  "type": "string"
               }
            ]
         }
      ]
   },
   "sink": {
      "type": "bigquery",
      "config": {
         "tables": [
            {
               "name": "gotest_dynamiccolumns",
               "dataset": "geisttest",
               "datasetCreation": {
                  "description": "cool dataset",
                  "location": "EU"
               },
               "insertIdFromId": "eventId",
               "columns": [
                  {
                     "name": "dateIngested",
                     "mode": "NULLABLE",
                     "type": "TIMESTAMP",
                     "description": "ingestion timestamp",
                     "valueFromId": "@GeistIngestionTime"
                  },
                  {
                     "nameFromId": {
                        "prefix": "",
                        "suffixFromId": "eventNameId"
                     },
                     "mode": "NULLABLE",
                     "type": "STRING",
                     "description": "raw event data",
                     "valueFromId": "rawEventId"
                  }
               ]
            }
         ]
      }
   }
}
`)

var streamSpecWithColumnCloneFeature = []byte(`
{
   "namespace": "geist",
   "streamIdSuffix": "xtobigquery_columncloning",
   "description": "Spec where BQ table is created with cloned columns from spec",
   "version": 1,
   "ops": {
      "logEventData": true
   },
   "source": {
      "type": "geistapi"
   },
   "transform": {
      "extractFields": [
         {
            "fields": [
               {
                  "id": "eventNameId",
                  "jsonPath": "name"
               },
               {
                  "id": "eventId",
                  "jsonPath": "eventId"
               },
               {
                  "id": "rawEventId",
                  "type": "string"
               }
            ]
         }
      ]
   },
   "sink": {
      "type": "bigquery",
      "config": {
         "tables": [
            {
               "name": "gotest_columncloning",
               "dataset": "geisttest",
               "insertIdFromId": "eventId",
               "columns": [
                  {
                     "name": "dateIngested",
                     "mode": "NULLABLE",
                     "type": "TIMESTAMP",
                     "description": "ingestion timestamp",
                     "valueFromId": "@GeistIngestionTime"
                  },
                  {
                     "nameFromId": {
                        "prefix": "",
                        "suffixFromId": "eventNameId",
                        "preset": ["col1", "col2"]
                     },
                     "mode": "NULLABLE",
                     "type": "STRING",
                     "description": "raw event data",
                     "valueFromId": "rawEventId"
                  }
               ]
            }
         ]
      }
   }
}
`)
