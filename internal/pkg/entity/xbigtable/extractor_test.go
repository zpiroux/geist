package xbigtable

import (
	"context"
	"testing"

	asrt "github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/internal/pkg/etltest"
	"github.com/zpiroux/geist/internal/pkg/model"
)

func TestExtractor_ExtractFromSink(t *testing.T) {

	applicableEvents := []string{testEventDir + "platform_change_event_ex1.json"}
	g := NewGeistTestSpecLoader(t, etltest.SpecApiSrcBigtableSinkMinimal)
	g.LoadEventsIntoSink(t, applicableEvents, "")

	extractor, err := NewExtractor(*g.Spec, g.Client, g.AdminClient)
	extractor.setOpenTables(m)
	asrt.NoError(t, err)
	asrt.NotNil(t, extractor)

	query := model.ExtractorQuery{
		Type: model.KeyValue,
		Key:  "foo",
	}

	var result []*model.Transformed

	err, _ = extractor.ExtractFromSink(context.Background(), query, &result)
	retryable := false
	asrt.NoErrorf(t, err, "retryable: %v", &retryable)

	tPrintf("Result from ExtractFromSink:\n")
	err = printTransformed(result)
	asrt.NoError(t, err)

	var keyValueResult []*model.Transformed
	query = model.ExtractorQuery{
		Type: model.KeyValue,
		Key:  "some_row_key",
	}

	err, _ = extractor.ExtractFromSink(context.Background(), query, &keyValueResult)

	// For now, this is on ok result, improve later
	asrt.NoErrorf(t, err, "retryable: %v", retryable)

	tPrintf("Result from ExtractFromSink (KeyValue):\n")
	err = printTransformed(keyValueResult)
	asrt.NoError(t, err)

}

func printTransformed(transformed []*model.Transformed) error {

	for i, trans := range transformed {
		tPrintf("Printing Transformed nb %d\n", i)
		tPrintf("%s\n", trans.String())
	}
	return nil
}
