package xbigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/teltech/logger"
	"github.com/zpiroux/geist/internal/pkg/model"
)

const (
	TableUpdateBackoffTime         = 8 * time.Second
	DefaultBigQueryDatasetLocation = "EU"
)

var log *logger.Log

func init() {
	log = logger.New()
}

type Loader struct {
	id       string
	spec     *model.Spec
	table    *bigquery.Table
	metadata *bigquery.TableMetadata
	client   BigQueryClient
	inserter BigQueryInserter
	mdMutex  *sync.Mutex
}

func NewLoader(
	ctx context.Context,
	spec *model.Spec,
	id string,
	client BigQueryClient,
	metadataMutex *sync.Mutex) (*Loader, error) {

	if isNil(client) {
		return nil, errors.New("invalid arguments, BigQueryClient cannot be nil")
	}
	l := Loader{
		id:      id,
		spec:    spec,
		client:  client,
		mdMutex: metadataMutex,
	}
	err := l.init(ctx)
	return &l, err
}

func (l *Loader) StreamLoad(ctx context.Context, data []*model.Transformed) (string, error, bool) {

	rows, newColumns, err := l.createRows(data)
	if err != nil {
		return "", err, false // This is probably an unretryable error due to corrupt schema or event
	}

	if len(rows) == 0 {
		log.Warnf(l.lgprfx()+"no rows to be inserted from transformed data, might be an error in stream spec: %+v", data)
		return "", nil, false
	}

	if err := l.handleDynamicColumnUpdates(ctx, newColumns); err != nil {
		return "", err, true
	}

	err = l.insertRows(ctx, rows)

	if err != nil && probableTableUpdatingError(err) {
		// If new column(s) been added by table update it can take some time before BQ allows inserts to the new
		// column. Until then insert will return with insert failed errors. To reduce request retries in this case
		// back off slightly.
		log.Warnf(l.lgprfx()+"BQ table probably not ready after table update, let's back off a few sec (err: %v)", err)
		if !sleepCtx(ctx, TableUpdateBackoffTime) {
			err = model.ErrEntityShutdownRequested
		}
	}

	if err == nil && l.spec.Ops.LogEventData {
		log.Debugf(l.lgprfx()+"successfully inserted %d rows to BigQuery table %s", len(rows), l.table.TableID)
	}

	return "", err, true
}

func probableTableUpdatingError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "no such field")
}

func (l *Loader) Shutdown() {}

func (l *Loader) init(ctx context.Context) error {

	var (
		err    error
		status DatasetTableStatus
	)

	if len(l.spec.Sink.Config.Tables) == 0 {
		return fmt.Errorf("no BigQuery table specified in spec: %v", l.spec)
	}
	tableSpec := l.spec.Sink.Config.Tables[0]
	l.table = l.client.CreateTableRef(tableSpec.Dataset, tableSpec.Name)

	// TODO: Add back-off-retry here, if init called from NewLoader()

	l.mdMutex.Lock()
	defer l.mdMutex.Unlock()

	_, status, err = l.client.GetDatasetMetadata(ctx, l.client.CreateDatasetRef(tableSpec.Dataset))
	if err != nil && status == Unknown {
		return err
	}
	if status == NonExistent {
		md := &bigquery.DatasetMetadata{
			Location: DefaultBigQueryDatasetLocation,
		}
		if tableSpec.DatasetCreation != nil {
			md.Description = tableSpec.DatasetCreation.Description
			if tableSpec.DatasetCreation.Location != "" {
				md.Location = tableSpec.DatasetCreation.Location
			}
		}
		if err := l.client.CreateDataset(ctx, tableSpec.Dataset, md); err != nil {
			return err
		}
	} else {
		log.Debugf(l.lgprfx()+"dataset %v already exists, no need to create it", tableSpec.Dataset)
	}

	l.metadata, status, err = l.client.GetTableMetadata(ctx, l.client.CreateTableRef(tableSpec.Dataset, tableSpec.Name))
	if err != nil && status == Unknown {
		return err
	}

	if status == NonExistent {
		l.metadata, err = l.createTableMetadata(ctx, tableSpec)
		if err != nil {
			return err
		}
		l.table, err = l.client.CreateTable(ctx, tableSpec.Dataset, tableSpec.Name, l.metadata)
	} else {
		log.Debugf(l.lgprfx()+"table %v already exists, no need to create it", l.table)
	}

	l.inserter = l.client.GetTableInserter(l.table)
	return err
}

// Creates table meta data based on config in stream spec, for use in table creation
func (l *Loader) createTableMetadata(ctx context.Context, tableSpec model.Table) (*bigquery.TableMetadata, error) {

	var columns []model.Column

	for _, col := range tableSpec.Columns {
		if col.Name != "" {
			columns = append(columns, col)
			continue
		}
		if col.NameFromId != nil {
			if len(col.NameFromId.Preset) > 0 {
				for _, colName := range col.NameFromId.Preset {
					newCol := col
					newCol.Name = colName
					columns = append(columns, newCol)
				}
			}
		}
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf(l.lgprfx() + "no columns could be generated from spec")
	}

	schemaJSON, err := json.Marshal(columns)
	if err != nil {
		return nil, err
	}
	schema, err := bigquery.SchemaFromJSON(schemaJSON)
	if err != nil || len(schema) == 0 {
		return nil, fmt.Errorf("could not create BigQuery schema from stream spec, err: %v", err)
	}

	md := &bigquery.TableMetadata{
		Schema: schema,
	}

	if tableSpec.TableCreation != nil {

		md.Description = tableSpec.TableCreation.Description
		md.RequirePartitionFilter = tableSpec.TableCreation.RequirePartitionFilter

		if len(tableSpec.TableCreation.Clustering) > 0 {
			md.Clustering = &bigquery.Clustering{Fields: tableSpec.TableCreation.Clustering}
		}

		if tableSpec.TableCreation.TimePartitioning != nil {
			md.TimePartitioning = &bigquery.TimePartitioning{
				Type:       bigquery.TimePartitioningType(tableSpec.TableCreation.TimePartitioning.Type),
				Expiration: time.Duration(tableSpec.TableCreation.TimePartitioning.ExpirationHours) * time.Hour,
				Field:      tableSpec.TableCreation.TimePartitioning.Field,
			}
		}
	}
	return md, err
}

type RowItem struct {
	Name  string
	Value any
}

type Columns map[string]model.Column

func (l *Loader) createRows(data []*model.Transformed) ([]*Row, Columns, error) {

	var (
		rows    []*Row
		skipRow bool
	)
	tableSpec := l.spec.Sink.Config.Tables[0]
	newColumns := make(Columns)

	// Each incoming Transformed map represents a possible row
	for _, rawRowData := range data {

		if skipRow {
			skipRow = false
			continue
		}
		row := NewRow()

		// Try to find each row item in the incoming row data item, based on column spec
		for _, col := range tableSpec.Columns {

			if skipRow {
				break
			}

			if value, ok := rawRowData.Data[col.ValueFromId]; ok {

				colName, err := getColumnName(col, rawRowData)
				if err != nil {
					return rows, nil, err
				}

				if colName == "" {
					log.Errorf(l.lgprfx()+"corrupt test event found for col: %+v, logged and disregarded: %v", col, rawRowData)
					skipRow = true
					break
				}

				if l.spec.Sink.Config.DiscardInvalidData {
					if errValidation := validateData(col, value); errValidation != nil {
						log.Warnf(l.lgprfx()+"invalid data found for col: %+v, err: %v, event logged and disregarded: %v", col, errValidation, rawRowData)
						skipRow = true
						break
					}
				}

				if !l.columnExists(colName) {
					newColumns[colName] = col
				}

				row.AddItem(&RowItem{
					Name:  colName,
					Value: value,
				})

			} else if col.ValueFromId == model.GeistIngestionTime {
				row.AddItem(&RowItem{
					Name:  col.Name,
					Value: time.Now().UTC(),
				})
			}
		}

		if row.Size() > 0 && !skipRow {
			if tableSpec.InsertIdFromId != "" {
				if insertId, ok := rawRowData.Data[tableSpec.InsertIdFromId]; ok {
					row.InsertId, ok = insertId.(string)
					if !ok {
						log.Errorf(l.lgprfx()+"corrupt insert ID in event with data %v, tableSpec: %+v", rawRowData, tableSpec)
					}
				}
			}
			rows = append(rows, row)
		}
	}

	return rows, newColumns, nil
}

func validateData(col model.Column, data any) error {

	var (
		err          error
		correctType  bool
		invalidValue bool
	)

	if col.Mode == "REQUIRED" {
		if isNil(data) {
			return fmt.Errorf("field mode set to REQUIRED but null value provided")
		}
	}
	switch col.Type {
	case string(bigquery.TimestampFieldType):
		switch data.(type) {
		case time.Time, nil:
			correctType = true
			if data.(time.Time).IsZero() {
				invalidValue = true
			}
		}
	case string(bigquery.BooleanFieldType), "BOOL":
		switch data.(type) {
		case bool, nil:
			correctType = true
		}
	case string(bigquery.IntegerFieldType), "INT64":
		switch data.(type) {
		case int, int32, int64, nil:
			correctType = true
		}
	case string(bigquery.StringFieldType):
		switch data.(type) {
		case string, nil:
			correctType = true
		}
	case string(bigquery.FloatFieldType), "FLOAT64", string(bigquery.NumericFieldType):
		switch data.(type) {
		case float64, float32, int, int32, int64, nil:
			correctType = true
		}
	case string(bigquery.BytesFieldType):
		switch data.(type) {
		case []byte, nil:
			correctType = true
		}
	default:
		// No data validation for RECORD, array/repeated fields
		correctType = true
	}
	if !correctType {
		err = fmt.Errorf("field type in schema: %s, does not match actual type: %T", col.Type, data)
	} else if invalidValue {
		err = fmt.Errorf("invalid field value: %v", data)
	}
	return err
}

func (l *Loader) handleDynamicColumnUpdates(ctx context.Context, newColumns Columns) error {

	if len(newColumns) == 0 {
		return nil
	}

	log.Infof(l.lgprfx()+"new columns found, to be created: %+v", newColumns)

	var newBqColumns bigquery.Schema

	for colName, col := range newColumns {
		newBqColumns = append(newBqColumns, &bigquery.FieldSchema{
			Name:        colName,
			Type:        bigquery.FieldType(col.Type),
			Description: col.Description,
			Repeated:    col.Mode == "REPEATED",
			// Required must be false for columns appended to a table

			// TODO: possibly add support for other options here later
		})
	}

	return l.addColumnsToTable(ctx, newBqColumns)
}

func (l *Loader) addColumnsToTable(ctx context.Context, newColumns bigquery.Schema) error {

	l.mdMutex.Lock()
	defer l.mdMutex.Unlock()

	// We cannot use the already stored metadata in the Loader since we need to get the etag from BQ
	// to ensure consistency in the Update operation.
	meta, _, err := l.client.GetTableMetadata(ctx, l.table)
	if err != nil {
		return err
	}

	update := bigquery.TableMetadataToUpdate{
		Schema: append(meta.Schema, newColumns...),
	}

	tm, err := l.client.UpdateTable(ctx, l.table, update, meta.ETag)

	if err == nil {
		// BQ takes a while to allow ingestion with new schema, this sleep will reduce number of retries,
		// although not required for actual functionality.
		if !sleepCtx(ctx, TableUpdateBackoffTime) {
			err = model.ErrEntityShutdownRequested
		}
		l.metadata = tm
	}

	log.Debugf(l.lgprfx()+"BQ update table returned err: %v", err)
	return err
}

func (l *Loader) columnExists(colName string) bool {

	if l.metadata != nil {
		for _, field := range l.metadata.Schema {
			if field.Name == colName {
				return true
			}
		}
	}
	return false
}

func getColumnName(col model.Column, rawRowData *model.Transformed) (string, error) {
	if col.Name != "" {
		return col.Name, nil
	}
	if col.NameFromId == nil {
		return "", fmt.Errorf("one of Name or NameFromId need to be present in column spec, col spec: %+v", col)
	}

	if value, ok := rawRowData.Data[col.NameFromId.SuffixFromId]; ok {

		v, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("invalid type in stream spec for col: %+v, transformed data: %s", col, rawRowData)
		}
		return col.NameFromId.Prefix + v, nil
	}
	return "", fmt.Errorf("column name not found, col spec: %+v, transformed data: %s", col, rawRowData)
}

func (l *Loader) insertRows(ctx context.Context, rows []*Row) error {

	return l.inserter.Put(ctx, rows)
}

func (l *Loader) lgprfx() string {
	return "[xbigquery.loader:" + l.id + "] "
}

type Row struct {
	InsertId string
	rowItems map[string]bigquery.Value
}

func NewRow() *Row {
	return &Row{
		rowItems: make(map[string]bigquery.Value),
	}
}

func (r *Row) AddItem(item *RowItem) {
	r.rowItems[item.Name] = item.Value
}

// Save is required for implementing the BigQuery ValueSaver interface, as used by the bigquery.Inserter
func (r *Row) Save() (map[string]bigquery.Value, string, error) {
	return r.rowItems, r.InsertId, nil
}

func (r *Row) Size() int {
	return len(r.rowItems)
}
