package xbigtable

import (
	"context"
	"reflect"

	"cloud.google.com/go/bigtable"
)

// Geist's BigTable Extractor and Loader uses GCP BigTable Go client API for its functionality.
// We're decoupling this API here on consumer side for full unit test capabilities.

type BigTableClient interface {
	Open(table string) *bigtable.Table
}

type BigTableAdminClient interface {
	Tables(ctx context.Context) ([]string, error)
	CreateTable(ctx context.Context, table string) error
	TableInfo(ctx context.Context, table string) (*bigtable.TableInfo, error)
	CreateColumnFamily(ctx context.Context, table, family string) error
	SetGCPolicy(ctx context.Context, table, family string, policy bigtable.GCPolicy) error
}

type BigTableTable interface {
	Apply(ctx context.Context, row string, m *bigtable.Mutation, opts ...bigtable.ApplyOption) (err error)

	// A missing row will return a zero-length map and a nil error.
	ReadRow(ctx context.Context, row string, opts ...bigtable.ReadOption) (bigtable.Row, error)
}

func isNil(v any) bool {
	return v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil())
}
