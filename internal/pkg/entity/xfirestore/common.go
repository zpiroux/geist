package xfirestore

import (
	"context"
	"reflect"

	"cloud.google.com/go/datastore"
)

// Geist's Firestore Extractor and Loader uses GCP Firestore Go client API for its functionality.
// We're decoupling this API here on consumer side for full unit test capabilities.

type FirestoreClient interface {
	Put(ctx context.Context, key *datastore.Key, src any) (*datastore.Key, error)
	Get(ctx context.Context, key *datastore.Key, dst any) (err error)
	GetAll(ctx context.Context, q *datastore.Query, dst any) (keys []*datastore.Key, err error)
}

func isNil(v any) bool {
	return v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil())
}
