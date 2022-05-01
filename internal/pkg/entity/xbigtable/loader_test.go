package xbigtable

import (
	"context"
	"fmt"
	"io/ioutil"
	"testing"

	"cloud.google.com/go/bigtable"
	_assert "github.com/stretchr/testify/assert"
	_require "github.com/stretchr/testify/require"
	"github.com/zpiroux/geist/internal/pkg/entity/transform"
	"github.com/zpiroux/geist/internal/pkg/etltest"
	"github.com/zpiroux/geist/internal/pkg/model"
)

const (
	testDirPath  = "../../../../test/"
	testSpecDir  = testDirPath + "specs/"
	testEventDir = testDirPath + "events/"
)

var (
	assert          *_assert.Assertions
	require         *_require.Assertions
	m               map[string]BigTableTable
	mockDb          map[string]any
	printTestOutput bool
)

func init() {
	mockDb = make(map[string]any)
	m = make(map[string]BigTableTable)
	m["some_user_data"] = &MockTable{}
	m["p_master_data"] = &MockTable{}
	m["s_master_data"] = &MockTable{}
	m["geisttest_apitobigtable"] = &MockTable{}
	m["geisttest_apitobigtable_fooround"] = &MockTable{}
	m["geisttest_featurex"] = &MockTable{}
}

type GeistTestSpecLoader struct {
	Registry    *etltest.StreamRegistry
	Spec        *model.Spec
	Client      *MockClient
	AdminClient *MockAdminClient
	Loader      *Loader
	Transformer *transform.Transformer
}

func NewGeistTestSpecLoader(t *testing.T, geistSpec string) *GeistTestSpecLoader {
	var g GeistTestSpecLoader
	assert = _assert.New(t)
	require = _require.New(t)

	ctx := context.Background()

	g.Registry = etltest.NewStreamRegistry(testDirPath)
	err := g.Registry.Fetch(ctx)
	require.NoError(err)

	spec, err := g.Registry.Get(ctx, geistSpec)
	require.NoError(err)
	require.NotNil(spec)
	g.Spec = spec.(*model.Spec)
	g.Client = &MockClient{}
	g.AdminClient = &MockAdminClient{}

	g.Loader, err = NewLoader(
		context.Background(),
		g.Spec,
		"mockId",
		g.Client,
		g.AdminClient)

	assert.NoError(err)
	assert.NotNil(g.Loader)
	g.Loader.setOpenTables(m)

	g.Transformer = transform.NewTransformer(g.Spec)

	return &g
}

func TestNewLoader(t *testing.T) {
	loader, err := NewLoader(context.Background(), &model.Spec{}, "mockId", nil, nil)
	_require.Error(t, err)
	_require.Nil(t, loader)
}

func TestLoader(t *testing.T) {

	printTestOutput = false

	// Single table test, GC Policy MaxVersions
	applicableEvents := []string{testEventDir + "foo_session_begin_ex1.json"}
	nonApplicableEvent := testEventDir + "platform_change_event_ex1.json"
	g := NewGeistTestSpecLoader(t, etltest.SpecKafkaSrcBigtableSinkPlayer)
	g.LoadEventsIntoSink(t, applicableEvents, nonApplicableEvent)

	// Multi-table test
	applicableEvents = []string{
		testEventDir + "foo_session_begin_ex1.json",
		testEventDir + "foo_session_end_ex1.json",
	}
	g = NewGeistTestSpecLoader(t, etltest.SpecKafkaSrcBigtableSinkMultiSession)
	g.LoadEventsIntoSink(t, applicableEvents, nonApplicableEvent)

	// Testing pre-defined row-keys
	applicableEvents = []string{testEventDir + "platform_change_event_ex1.json"}
	g = NewGeistTestSpecLoader(t, etltest.SpecApiSrcBigtableSinkMinimal)
	g.LoadEventsIntoSink(t, applicableEvents, "")

	// Test dynamic generation of column names from fields in events
	applicableEvents = []string{testEventDir + "foo_round_tracking.json"}
	g = NewGeistTestSpecLoader(t, etltest.SpecApiSrcBigtableSinkFooRound)
	g.LoadEventsIntoSink(t, applicableEvents, "")
	assert.Equal("a9f5e7e7-6020-4eb8-ad16-00e27c3b1ab6", mockDb["a9f5e7e7-6020-4eb8-ad16-00e27c3b1ab6"])

	// Test JSON array transform and keysInMap predefined row-key generation
	applicableEvents = []string{testEventDir + "featurex_config_snapshot.json"}
	g = NewGeistTestSpecLoader(t, etltest.SpecKafkaSrcBigtableSinkFeatureX)
	g.LoadEventsIntoSink(t, applicableEvents, "")
	assert.Equal("cust1#prod_y", mockDb["cust1#prod_y"])
	assert.Equal("cust2#prod_x", mockDb["cust2#prod_x"])
}

func TestInvertedTimestamp(t *testing.T) {
	inverted := invertedTimestamp()
	fmt.Println(inverted)
	assert.NotEmpty(inverted)
}

func (g *GeistTestSpecLoader) LoadEventsIntoSink(t *testing.T, applicableEvents []string, nonApplicableEvent string) {

	retryable := false
	ctx := context.Background()

	for _, event := range applicableEvents {
		fileBytes, err := ioutil.ReadFile(event)
		assert.NoError(err)
		output, err := g.Transformer.Transform(context.Background(), fileBytes, &retryable)
		assert.NoError(err)
		assert.NotNil(output)

		_, err, retryable = g.Loader.StreamLoad(ctx, output)
		assert.NoError(err)
	}

	// Test with a non-applicable event, transform output should be nil
	if len(nonApplicableEvent) > 0 {
		fileBytes, err := ioutil.ReadFile(nonApplicableEvent)
		assert.NoError(err)
		output, err := g.Transformer.Transform(context.Background(), fileBytes, &retryable)
		assert.NoError(err)
		assert.Nil(output)

		_, err, retryable = g.Loader.StreamLoad(ctx, output)
		assert.Error(err)
	}
}

type MockClient struct{}

func (m *MockClient) Open(table string) *bigtable.Table {
	assert.NotEmpty(table)
	return &bigtable.Table{}
}

type MockAdminClient struct{}

func (m *MockAdminClient) Tables(ctx context.Context) ([]string, error) {
	return []string{"foo", "bar"}, nil
}

func (m *MockAdminClient) CreateTable(ctx context.Context, table string) error {
	assert.NotEmpty(table)
	return nil
}
func (m *MockAdminClient) TableInfo(ctx context.Context, table string) (*bigtable.TableInfo, error) {
	assert.NotEmpty(table)
	return &bigtable.TableInfo{}, nil
}

func (m *MockAdminClient) CreateColumnFamily(ctx context.Context, table string, family string) error {
	assert.NotEmpty(table)
	assert.NotEmpty(family)
	return nil
}
func (m *MockAdminClient) SetGCPolicy(ctx context.Context, table string, family string, policy bigtable.GCPolicy) error {
	assert.NotEmpty(table)
	assert.NotEmpty(family)
	assert.NotNil(policy)
	tPrintf("Setting GC Policy for table: %s, family: %s, policy: %v\n", table, family, policy)
	return nil
}

type MockTable struct{}

func (mt *MockTable) Apply(ctx context.Context, rowKey string, m *bigtable.Mutation, opts ...bigtable.ApplyOption) (err error) {
	assert.NotEmpty(rowKey)
	assert.NotNil(m)
	tPrintf("MockTable.Apply: row: %s, mutation: %+v\n", rowKey, *m)
	mockDb[rowKey] = rowKey // can't access internal BT mutation struct data easily, can only verify that correct rowKey is set
	return nil
}

func (mt *MockTable) ReadRow(ctx context.Context, rowKey string, opts ...bigtable.ReadOption) (bigtable.Row, error) {
	assert.NotEmpty(rowKey)
	var readItems []bigtable.ReadItem

	row := make(map[string][]bigtable.ReadItem)

	readItem := bigtable.ReadItem{
		Row:    "mockrow",
		Column: "mockcolumn",
		Value:  []byte("mock value"),
	}

	readItems = append(readItems, readItem)
	row["mockColumnFamily"] = readItems
	return row, nil
}

func tPrintf(format string, a ...any) {
	if printTestOutput {
		fmt.Printf(format, a...)
	}
}
