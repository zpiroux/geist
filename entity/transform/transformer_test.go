package transform

import (
	"context"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/zpiroux/geist/entity"
)

const (
	testSpecDir  = "../../test/specs/"
	testEventDir = "../../test/events/"
)

var (
	rawEvent = "{\"foo\": {\"evtType\": \"FOO_SESSION_BEGIN\",\"ts\": [{\"label\": \"someLabel\",\"time\": 1574608103112}],\"evtVer\": \"1.10\",\"evtId\": \"1c9fa7a0-f475-4ab5-b62b-073f4551e5f1\"},\"bar\": {\"stuff\": {\"sid\": \"0f4b8df8-f6e6-4432-a52d-05692b3a5c58\",\"pInfo\": {\"pName\": \"somename\",\"pId\": 82348,\"pCur\": \"SEK\"}}}}"

	printTestOutput bool
)

func TestTransformer(t *testing.T) {

	var (
		fileBytes []byte
		err       error
		retryable bool
		output    []*entity.Transformed
	)

	printTestOutput = false

	// Validate general parsing and handling of specs and events
	fileBytes, err = ioutil.ReadFile(testSpecDir + "kafkasrc-bigtablesink-multitable-session.json")
	assert.NoError(t, err)
	spec, err := entity.NewSpec(fileBytes)
	assert.NoError(t, err)
	require.NotNil(t, spec)

	transformer := NewTransformer(spec)

	fileBytes, _ = ioutil.ReadFile(testEventDir + "foo_session_begin_ex1.json")
	output, err = transformer.Transform(context.Background(), fileBytes, &retryable)
	assert.NoError(t, err)
	require.NotNil(t, output)
	tPrintf("Transformation output: %+v\n", output)

	fileBytes, _ = ioutil.ReadFile(testEventDir + "foo_session_end_ex1.json")
	output, err = transformer.Transform(context.Background(), fileBytes, &retryable)
	assert.NoError(t, err)
	require.NotNil(t, output)
	tPrintf("Transformation output: %+v\n", output)

	output, err = transformer.Transform(context.Background(), []byte(rawEvent), &retryable)
	assert.NoError(t, err)
	require.NotNil(t, output)
	tPrintf("Transformation output: %+v\n", output)

}

func TestTransformer_Regexp(t *testing.T) {
	var (
		fileBytes []byte
		output    []*entity.Transformed
		err       error
	)

	printTestOutput = false

	// RegExp spec 1
	sdJson := "{\"insertId\":\"a6bf3a8d-4fe0-40d9-bfce-0ebe5bdbdb86\",\"labels\":{\"foo\":\"bar\"},\"logName\":\"fooservice/accesslog\",\"rcvTimestamp\":\"2020-06-16T12:06:31.869709059Z\",\"textPayload\":\"cust1-loc1.somesite.com|11.222.123.123|https://<lots more stuff>|<ua info...>|-|-|-|[17/Jun/2020:09:10:25 +0200]<|GET /some/reqPath;more-stuff... HTTP/1.1|200|996|19\",\"timestamp\":\"2020-06-16T12:06:26.723709116Z\"}"
	fileBytes, err = ioutil.ReadFile(testSpecDir + "pubsubsrc-regexp-reqs-voidsink.json")
	assert.NoError(t, err)
	spec, err := entity.NewSpec(fileBytes)
	assert.NoError(t, err)
	transformer := NewTransformer(spec)
	retryable := false
	output, err = transformer.Transform(context.Background(), []byte(sdJson), &retryable)
	assert.NoError(t, err)
	tPrintf("Transformation output: %+v\n", output)
	assert.Len(t, output, 1)
	out := output[0].Data["regexppayload"]
	expected := "{\"customer\":\"cust1\",\"httpResponse\":\"200\",\"httpVerb\":\"GET\",\"ip\":\"11.222.123.123\",\"port\":\"\",\"reqLoc\":\"loc1\",\"reqPath\":\"/some/reqPath\",\"ts\":\"2020-06-17T09:10:25+02:00\"}"
	assert.Equal(t, expected, string(out.([]byte)))

	spec.Transform.ExtractFields = []entity.ExtractFields{}
	transformer = NewTransformer(spec)
	output, err = transformer.Transform(context.Background(), []byte(sdJson), &retryable)
	assert.NoError(t, err)
	tPrintf("Transformation output: %+v\n", output)
	assert.Nil(t, output)

	// RegExp spec 2
	sdJson = "{\"insertId\":\"d5696f71-9202-45e4-ba9d-40d467fb7516\",\"labels\":{\"foo\":\"bar\"},\"logName\":\"fooservice/accesslog\",\"rcvTimestamp\":\"2020-06-16T12:06:31.869709059Z\",\"textPayload\":\"2020-07-01 16:06:57,695 +0200 INFO  [LOG_cust2.BarService.getUserInfo] (HTTP-126) Invocation took: 493 ms (492835106 ns)\",\"timestamp\":\"2020-06-16T12:06:26.723709116Z\"}"
	fileBytes, err = ioutil.ReadFile(testSpecDir + "pubsubsrc-regexp-barusage-voidsink.json")
	assert.NoError(t, err)
	spec, err = entity.NewSpec(fileBytes)
	assert.NoError(t, err)
	transformer = NewTransformer(spec)
	output, err = transformer.Transform(context.Background(), []byte(sdJson), &retryable)
	assert.NoError(t, err)
	tPrintf("Transformation output: %+v\n", output)
	assert.NotNil(t, output)
	out = output[0].Data["regexppayload"]
	expected = "{\"customer\":\"cust2\",\"logLevel\":\"INFO\",\"method\":\"getUserInfo\",\"responseTime\":\"493\",\"ts\":\"2020-07-01T16:06:57+02:00\"}"
	assert.Equal(t, expected, string(out.([]byte)))
}

func TestTransformer_TimeConv(t *testing.T) {
	var tcSpec entity.TimeConv
	tcSpec.InputFormat = "2006-01-02 03:04:05.999 -0700"
	tcSpec.Field = "ts"

	td := entity.NewTransformed()
	td.Data["ts"] = "2020-07-01 12:23:03,494 +0200"
	date, err := timeConv(&tcSpec, td.Data["ts"].(string))
	assert.NoError(t, err)
	assert.Equal(t, "2020-07-01T12:23:03+02:00", date)

	tcSpec.InputFormat = "02/Jan/2006:15:04:05 -0700"
	tcSpec.Field = "timestamp"

	td = entity.NewTransformed()
	td.Data["timestamp"] = "01/Jul/2020:13:21:37 +0200"
	date, err = timeConv(&tcSpec, td.Data["timestamp"].(string))
	assert.NoError(t, err)
	assert.Equal(t, "2020-07-01T13:21:37+02:00", date)
}

func TestTransformer_ExtractFields(t *testing.T) {

	var err error

	printTestOutput = false

	json := `{"coolNumber": "333"}`
	fields := []entity.Field{
		{
			Id:       "myCoolNumber",
			JsonPath: "coolNumber",
			Type:     "integer",
		},
	}
	ef := entity.ExtractFields{
		Fields: fields,
	}
	transformed, err := extractFields(ef, []byte(json))
	assert.NoError(t, err)
	assert.NotNil(t, transformed)
	tPrintf("transformed: %s\n", transformed.String())

	fields = []entity.Field{
		{
			Id:       "myCoolNumber",
			JsonPath: "coolNumber",
		},
	}
	ef = entity.ExtractFields{
		Fields: fields,
	}
	transformed, err = extractFields(ef, []byte(json))
	assert.NoError(t, err)
	assert.NotNil(t, transformed)

	// Verify a json int is stored as string in output map if type is not set
	json = `{"coolNumber": 333}`
	fields = []entity.Field{
		{
			Id:       "myCoolNumber",
			JsonPath: "coolNumber",
		},
	}
	ef = entity.ExtractFields{
		Fields: fields,
	}
	transformed, err = extractFields(ef, []byte(json))
	assert.NoError(t, err)
	assert.NotNil(t, transformed)
	tPrintf("transformed: %s\n", transformed)
	coolNumber, ok := transformed.Data["myCoolNumber"]
	assert.True(t, ok)
	assert.Equal(t, "333", coolNumber)

	// Test handling of timestamp conversions
	json = `{"coolIsoTimestamp": "2019-11-30T14:57:23.389Z"}`
	fields = []entity.Field{
		{
			Id:       "myIsoTimestamp",
			JsonPath: "coolIsoTimestamp",
			Type:     "string",
		},
	}
	ef = entity.ExtractFields{
		Fields: fields,
	}
	transformed, err = extractFields(ef, []byte(json))
	assert.NoError(t, err)
	assert.NotNil(t, transformed)
	tPrintf("transformed: %#v\n", transformed)

	json = `{"coolIsoTimestamp": "2019-11-30T14:57:23.389Z"}`
	fields = []entity.Field{
		{
			Id:       "myIsoTimestamp",
			JsonPath: "coolIsoTimestamp",
			Type:     "isoTimestamp",
		},
	}
	ef = entity.ExtractFields{
		Fields: fields,
	}
	transformed, err = extractFields(ef, []byte(json))
	assert.NoError(t, err)
	assert.NotNil(t, transformed)
	tPrintf("transformed: %#v\n", transformed)

	json = `{"coolUnixTimestamp": 1571831226950}`
	fields = []entity.Field{
		{
			Id:       "myUnixTimestamp",
			JsonPath: "coolUnixTimestamp",
			Type:     "unixTimestamp",
		},
	}
	ef = entity.ExtractFields{
		Fields: fields,
	}
	transformed, err = extractFields(ef, []byte(json))
	assert.NoError(t, err)
	assert.NotNil(t, transformed)
	tPrintf("transformed: %s", transformed.String())

	json = `{"coolUnixTimestamp": "1571831226959"}`
	fields = []entity.Field{
		{
			Id:       "myUnixTimestamp",
			JsonPath: "coolUnixTimestamp",
			Type:     "unixTimestamp",
		},
	}
	ef = entity.ExtractFields{
		Fields: fields,
	}
	transformed, err = extractFields(ef, []byte(json))
	assert.NoError(t, err)
	assert.NotNil(t, transformed)
	tPrintf("transformed: %s", transformed.String())

	// Test full event data transform
	json = `{"myThing": "1", "myOtherThing": 2}`
	fields = []entity.Field{
		{
			Id:       "myFullRawEvent",
			JsonPath: "",
			Type:     "string",
		},
	}
	ef = entity.ExtractFields{
		Fields: fields,
	}
	transformed, err = extractFields(ef, []byte(json))
	assert.NoError(t, err)
	assert.NotNil(t, transformed)
	tPrintf("transformed: %s\n", transformed.String())

	json = `{"cloudyWeather": true, "rainyWeather": false}`
	fields = []entity.Field{
		{
			Id:       "cloudy",
			JsonPath: "cloudyWeather",
			Type:     "bool",
		},
		{
			Id:       "rainy",
			JsonPath: "rainyWeather",
			Type:     "bool",
		},
	}
	ef = entity.ExtractFields{
		Fields: fields,
	}
	transformed, err = extractFields(ef, []byte(json))
	assert.NoError(t, err)
	assert.NotNil(t, transformed)
	tPrintf("transformed: %s", transformed.String())

	json = `{"amount": 92834.37}`
	fields = []entity.Field{
		{
			Id:       "amount",
			JsonPath: "amount",
			Type:     "float",
		},
	}
	ef = entity.ExtractFields{
		Fields: fields,
	}
	transformed, err = extractFields(ef, []byte(json))
	assert.NoError(t, err)
	assert.NotNil(t, transformed)
	tPrintf("transformed: %s", transformed.String())
}

func TestJsonBlobExtract(t *testing.T) {
	json := `{"unimportantStuff": "foo", "importantStuff": {"field1": "value1", "field2": "value2"}}`
	fields := []entity.Field{
		{
			Id:       "jsonBlobId",
			JsonPath: "importantStuff",
			Type:     "string",
		},
	}
	ef := entity.ExtractFields{
		Fields: fields,
	}
	transformed, err := extractFields(ef, []byte(json))
	assert.NoError(t, err)
	assert.NotNil(t, transformed)
	jsonBlob, ok := transformed.Data["jsonBlobId"]
	assert.True(t, ok)
	assert.Equal(t, "{\"field1\": \"value1\", \"field2\": \"value2\"}", jsonBlob)
}

func TestUserAgentExtract(t *testing.T) {
	json := `{"ua": "Mozilla%2F5.0%20(iPhone%3B%20CPU%20iPhone%20OS%2014_6%20like%20Mac%20OS%20X)%20AppleWebKit%2F605.1.15%20(KHTML%2C%20like%20Gecko)%20Version%2F14.1.1%20Mobile%2F15E148%20Safari%2F604.1"}`
	fields := []entity.Field{
		{
			Id:       "userAgentId",
			JsonPath: "ua",
			Type:     "userAgent",
		},
	}
	ef := entity.ExtractFields{
		Fields: fields,
	}
	transformed, err := extractFields(ef, []byte(json))
	assert.NoError(t, err)
	assert.NotNil(t, transformed)
	uaJson, ok := transformed.Data["userAgentId"]
	assert.True(t, ok)
	assert.Equal(t, `{"platform":"iPhone","operatingSystem":{"name":"iPhone OS","fullName":"CPU iPhone OS 14_6 like Mac OS X","version":"14.6"},"localization":"","browser":{"name":"Safari","version":"14.1.1","engine":"AppleWebKit","engineVersion":"605.1.15"},"bot":false,"mobile":true}`, uaJson)
}

var (
	applicableErsEvent    = "{\"name\":\"XCH_RATES_UPDATED\",\"version\":\"1.0\",\"ts\":\"2099-12-07T23:21:43.735Z\",\"id\":\"c84fc871-c8cb-4c8b-8a09-f4ba969ac843\",\"data\":[{\"base\":\"EUR\",\"rates\":{\"HRK\":0.14726,\"CHF\":1}}]}"
	nonApplicableErsEvent = "{\"name\":\"XCH_RATES_UPDATED\",\"version\":\"1.0\",\"ts\":\"2059-12-07T23:21:43.735Z\",\"id\":\"c84fc871-c8cb-4c8b-8a09-f4ba969ac843\",\"data\":[{\"base\":\"CHF\",\"rates\":{\"HRK\":0.14726,\"CHF\":1}}]}"
)

func TestTransformer_ArrayConditionals(t *testing.T) {

	var (
		fileBytes []byte
		err       error
		retryable bool
		output    []*entity.Transformed
	)

	printTestOutput = false

	fileBytes, err = ioutil.ReadFile(testSpecDir + "kafkasrc-bigtablesink-xch-eur.json")
	assert.NoError(t, err)
	spec, err := entity.NewSpec(fileBytes)
	assert.NoError(t, err)
	require.NotNil(t, spec)

	transformer := NewTransformer(spec)

	fileBytes, _ = ioutil.ReadFile(testEventDir + "xch_rates_updated.json")
	output, err = transformer.Transform(context.Background(), fileBytes, &retryable)
	assert.NoError(t, err)
	require.NotNil(t, output)
	require.Equal(t, "2019-12-07T13:21:42.615Z", output[0].Data["eventDate"])
	tPrintf("Transformation output: %+v\n", output)

	output, err = transformer.Transform(context.Background(), []byte(nonApplicableErsEvent), &retryable)
	assert.NoError(t, err)
	require.Nil(t, output)

	output, err = transformer.Transform(context.Background(), []byte(applicableErsEvent), &retryable)
	assert.NoError(t, err)
	require.NotNil(t, output)
	require.Equal(t, output[0].Data["eventDate"], "2099-12-07T23:21:43.735Z")
	tPrintf("Transformation output: %+v\n", output)
}

func tPrintf(format string, a ...any) {
	if printTestOutput {
		fmt.Printf(format, a...)
	}
}

func TestTransformer_TransformedItemsFromJsonArray(t *testing.T) {

	printTestOutput = false
	json := `
		{
  			"name": "COOL_EVENT",
  			"dateOccurred": "2020-09-07T13:45:44.559Z",
  			"coolArray": [
    			{
      				"fooId": "fooValue1",
      				"barId": "barValue1",
      				"things": "x",
      				"moreThings": ["3","2","1"]
    			},
    			{
      				"fooId": "fooValue2",
      				"barId": "barValue2",
      				"things": "x",
      				"moreThings": ["6","5","4"]
    			}
  			]
		}`
	itemSpec := entity.ExtractItemsFromArray{
		Id: "myItemMapId",
		Items: entity.ArrayItems{
			JsonPathToArray:  "coolArray",
			IdFromItemFields: entity.IdFromItemFields{Delimiter: "#", Fields: []string{"fooId", "barId"}},
		},
	}

	transformed, err := extractItemsFromArray(itemSpec, []byte(json))
	assert.NoError(t, err)
	assert.NotNil(t, transformed)
	items := transformed.Data["myItemMapId"].(map[string]any)
	assert.Equal(t, 2, len(items))
	value, ok := items["fooValue1#barValue1"]
	assert.True(t, ok)
	result := gjson.Get(json, "coolArray.0")
	assert.Equal(t, result.Raw, value)
	value, ok = items["fooValue2#barValue2"]
	assert.True(t, ok)
	result = gjson.Get(json, "coolArray.1")
	assert.Equal(t, result.Raw, value)
	tPrintf("transformed: %s\n", transformed)

	// Testing with full stream spec
	var (
		fileBytes []byte
		retryable bool
		output    []*entity.Transformed
	)
	fileBytes, err = ioutil.ReadFile(testSpecDir + "kafkasrc-bigtablesink-featurex.json")
	assert.NoError(t, err)
	spec, err := entity.NewSpec(fileBytes)
	assert.NoError(t, err)
	assert.NotNil(t, spec)

	transformer := NewTransformer(spec)
	fileBytes, _ = ioutil.ReadFile(testEventDir + "featurex_config_snapshot.json")
	output, err = transformer.Transform(context.Background(), fileBytes, &retryable)
	items = output[0].Data["arrayItemsMapId"].(map[string]any)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	value, ok = items["cust1#prod_y"]
	assert.True(t, ok)
	assert.NotEmpty(t, value.(string))
	value, ok = items["cust2#prod_x"]
	assert.True(t, ok)
	assert.NotEmpty(t, value.(string))
	tPrintf("Transformation output: %+v\n", output)
}

func TestTransformer_ExcludeEvents(t *testing.T) {

	specJson := []byte(`
	{
   		"namespace": "geisttest",
   		"streamIdSuffix": "xcludeevents",
   		"version": 1,
   		"description": "...",
   		"source": {
      		"type": "geistapi"
   		},
   		"transform": {
      		"excludeEventsWith": [
         		{
            		"key": "name",
            		"values": ["USELESS_EVENT", "BORING_EVENT"]
         		},
         		{
            		"key": "provider",
            		"values": ["unreliableService"]
         		}
      		],
      		"extractFields": [
         		{
            		"fields": [
     	          		{
        	          		"id": "rawEvent",
            	      		"type": "string"
               			}
            		]
         		}
      		]
   		},
   		"sink": {
      		"type": "void"
   		}
	}
	`)

	retryable := false
	spec, err := entity.NewSpec(specJson)
	assert.NoError(t, err)
	require.NotNil(t, spec)
	transformer := NewTransformer(spec)

	uselessEvent := []byte(`
	{
  		"name": "USELESS_EVENT",
  		"dateOccurred": "2020-12-13T00:45:44.559Z",
	}`)

	// Event should be excluded (output == nil, err == nil)
	output, err := transformer.Transform(context.Background(), uselessEvent, &retryable)
	assert.Nil(t, output)
	assert.NoError(t, err)

	boringEvent := []byte(`
	{
  		"name": "BORING_EVENT",
  		"dateOccurred": "2020-12-13T01:45:00.456Z",
	}`)

	// Event should be excluded (output == nil, err == nil)
	output, err = transformer.Transform(context.Background(), boringEvent, &retryable)
	assert.Nil(t, output)
	assert.NoError(t, err)

	greatEvent := []byte(`
	{
  		"name": "GREAT_EVENT",
  		"dateOccurred": "2020-12-13T01:45:00.456Z",
	}`)

	// Event should not be excluded (output != nil, err == nil)
	output, err = transformer.Transform(context.Background(), greatEvent, &retryable)
	assert.NotNil(t, output)
	assert.NoError(t, err)

	greatEventFromUnreliableService := []byte(`
	{
  		"name": "GREAT_EVENT",
  		"dateOccurred": "2020-12-13T01:45:00.456Z",
        "provider": "unreliableService"
	}`)

	// Event should be excluded (output != nil, err == nil)
	output, err = transformer.Transform(context.Background(), greatEventFromUnreliableService, &retryable)
	assert.Nil(t, output)
	assert.NoError(t, err)
}
