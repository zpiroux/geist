package xfirestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zpiroux/geist/internal/pkg/entity/transform"
	"github.com/zpiroux/geist/internal/pkg/etltest"
	"github.com/zpiroux/geist/internal/pkg/model"
)

const (
	testDirPath  = "../../../../test/"
	testSpecDir  = testDirPath + "specs/"
	testEventDir = testDirPath + "events/"
)

var printTestOutput bool

func TestLoader(t *testing.T) {

	printTestOutput = false

	g := NewGeistTestSpecLoader(t)

	g.LoadEventIntoSink(t, testSpecDir+"pubsubsrc-kafkasink-foologs.json")
	assert.Equal(t, 1, g.Client.numberOfEntities())

	g.LoadEventIntoSink(t, testSpecDir+"kafkasrc-bigtablesink-user.json")
	assert.Equal(t, 2, g.Client.numberOfEntities())

	g.LoadEventIntoSink(t, testSpecDir+"apisrc-bigtablesink-fooround.json")
	assert.Equal(t, 3, g.Client.numberOfEntities())

	g.ValidateLoadedEventData(t)

}

type GeistTestSpecLoader struct {
	Registry    *etltest.StreamRegistry
	Spec        *model.Spec
	Client      *MockClient
	Loader      *Loader
	Transformer *transform.Transformer
}

func NewGeistTestSpecLoader(t *testing.T) *GeistTestSpecLoader {
	var g GeistTestSpecLoader

	ctx := context.Background()

	g.Registry = etltest.NewStreamRegistry(testDirPath)
	err := g.Registry.Fetch(ctx)
	require.NoError(t, err)

	spec, err := g.Registry.Get(ctx, etltest.SpecRegSpecPubsub)
	require.NoError(t, err)
	require.NotNil(t, spec)

	g.Spec = spec.(*model.Spec)
	g.Client = &MockClient{}

	g.Loader, err = NewLoader(
		g.Spec,
		"mockId",
		g.Client,
		"coolDefaultNamespaceName")

	assert.NoError(t, err)
	assert.NotNil(t, g)
	log.Infof("Loader status: %#v", g)

	g.Transformer = transform.NewTransformer(spec.(*model.Spec))

	return &g
}

func (g *GeistTestSpecLoader) LoadEventIntoSink(t *testing.T, eventInFile string) {
	var retryable bool
	fileBytes, err := ioutil.ReadFile(eventInFile)
	assert.NoError(t, err)
	output, err := g.Transformer.Transform(context.Background(), fileBytes, &retryable)
	assert.NoError(t, err)
	require.NotNil(t, output)
	tPrintf("Transformation output, len: %d\n", len(output))

	_, err, retryable = g.Loader.StreamLoad(context.Background(), output)
	assert.NoError(t, err)
}

// Checks if the loaded spec JSONs can be retrieved and unmarshaled back into GEIST Spec Structs
func (g *GeistTestSpecLoader) ValidateLoadedEventData(t *testing.T) {

	var propLists []datastore.PropertyList

	keys, err := g.Client.GetAll(context.Background(), &datastore.Query{}, &propLists)
	assert.NoError(t, err)
	assert.NotNil(t, keys)

	tPrintf("Found the following loaded events (entities):\n")
	for i, propList := range propLists {

		printPropertyList(keys[i], &propList)

		// Get the specData value and unmarshal into Spec struct
		err := validateSpecWithModel(&propList)
		assert.NoError(t, err)
	}
}

func validateSpecWithModel(p *datastore.PropertyList) error {

	var spec model.Spec
	for _, prop := range *p {
		pn := prop.Name
		if pn == "specData" {
			pv := prop.Value.(string) // Note that this test assumes spec data to be stored as string

			if err := json.Unmarshal([]byte(pv), &spec); err != nil {
				return err
			}
			tPrintf("property with name: '%s' with data unmashalled into '%+v'\n", pn, spec)
		}
	}
	return nil
}

type MockClient struct {
	Keys     []*datastore.Key
	Entities []*datastore.PropertyList
}

func (m *MockClient) Put(ctx context.Context, key *datastore.Key, src any) (*datastore.Key, error) {

	tPrintf("%s\n", "In MockClient.Put()")

	data := src.(*datastore.PropertyList)

	printPropertyList(key, data)

	m.Keys = append(m.Keys, key)
	m.Entities = append(m.Entities, data)

	return key, nil
}

func (m *MockClient) Get(ctx context.Context, key *datastore.Key, dst any) (err error) {

	if dst == nil {
		return datastore.ErrInvalidEntityType
	}
	result := dst.(*datastore.PropertyList)

	for i, storedKey := range m.Keys {
		if key.Name == storedKey.Name {
			*result = *m.Entities[i]
			return nil
		}
	}

	return errors.New("not found")
}

func (m *MockClient) GetAll(ctx context.Context, q *datastore.Query, dst any) (keys []*datastore.Key, err error) {

	dv := reflect.ValueOf(dst)
	if dv.Kind() != reflect.Ptr || dv.IsNil() {
		return nil, datastore.ErrInvalidEntityType
	}

	result := dst.(*[]datastore.PropertyList)

	for _, entity := range m.Entities {
		*result = append(*result, *entity)
	}
	return m.Keys, nil
}

func (m *MockClient) numberOfEntities() int {
	return len(m.Entities)
}

func printPropertyList(key *datastore.Key, p *datastore.PropertyList) {
	tPrintf("Entity with key: '%+v' has the following properties:\n", *key)
	for _, prop := range *p {
		pn := prop.Name
		pv := prop.Value.(string) // Note that this assumes the value to be of type string, which is not the case always
		tPrintf("property with name: '%s' and value '%s'\n", pn, pv)
	}
}

func tPrintf(format string, a ...any) {
	if printTestOutput {
		fmt.Printf(format, a...)
	}
}
