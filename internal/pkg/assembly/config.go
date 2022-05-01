package assembly

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
	"github.com/zpiroux/geist/internal/pkg/model"
)

const (
	kafkaProviderConfluent = "confluent"
	kafkaProviderNative    = "native"
)

type Config struct {
	Env       model.Environment
	Kafka     KafkaConfig
	BigTable  BigTableConfig
	BigQuery  BigQueryConfig
	Firestore FirestoreConfig
	Pubsub    PubsubConfig
}

func (c Config) Close() error {

	var err error

	errBT := c.BigTable.Close()
	errBQ := c.BigQuery.Close()

	if errBT != nil || errBQ != nil {
		err = fmt.Errorf("errBT: %v, errBQ: %v", errBT, errBQ)
	}

	return err
}

type KafkaConfig struct {
	Enabled                  bool
	BootstrapServers         string // Default servers, can be overriden in GEIST specs
	ConfluentBootstrapServer string // Default server, can be overriden in GEIST specs
	ConfluentApiKey          string
	ConfluentApiSecret       string     `json:"-"`
	PollTimeoutMs            int        // Default poll timeout, can be overridden in GEIST specs
	QueuedMaxMessagesKb      int        // Events consumed and processed before commit
}

// Convenience function for easy default config
func (k KafkaConfig) DefaultBootstrapServers(provider string) string {
	if provider == kafkaProviderConfluent {
		return k.ConfluentBootstrapServer
	}
	return k.BootstrapServers
}

type BigTableConfig struct {
	Enabled     bool
	InstanceId  string
	Client      *bigtable.Client
	AdminClient *bigtable.AdminClient
}

func (b *BigTableConfig) InitClients(ctx context.Context, projectId string) (err error) {
	if !b.Enabled {
		return nil
	}
	if b.InstanceId == "" {
		return errors.New("no BigTable instance id set")
	}
	if b.Client, err = bigtable.NewClient(ctx, projectId, b.InstanceId); err != nil {
		return err
	}
	if b.AdminClient, err = bigtable.NewAdminClient(ctx, projectId, b.InstanceId); err != nil {
		return err
	}
	return nil
}

func (b BigTableConfig) Close() error {
	errorStr := ""
	if b.Client != nil {
		if err := b.Client.Close(); err != nil {
			errorStr = err.Error()
		}
	}
	if b.AdminClient != nil {
		if err := b.AdminClient.Close(); err != nil {
			errorStr += err.Error()
		}
	}
	if errorStr != "" {
		return fmt.Errorf("error(s) closing BigTable client(s): %s", errorStr)
	}
	return nil
}

type BigQueryConfig struct {
	Enabled       bool
	Client        *bigquery.Client
}

func (b *BigQueryConfig) InitClient(ctx context.Context, projectId string) (err error) {
	if b.Enabled {
		b.Client, err = bigquery.NewClient(ctx, projectId)
	}
	return err
}

func (b BigQueryConfig) Close() error {
	return b.Client.Close()
}

type FirestoreConfig struct {
	Enabled bool
	Client  *datastore.Client
}

func (f *FirestoreConfig) InitClient(ctx context.Context, projectId string) (err error) {
	if f.Enabled {
		f.Client, err = datastore.NewClient(ctx, projectId)
	}
	return err
}

type PubsubConfig struct {
	Enabled                bool
	Client                 *pubsub.Client
	MaxOutstandingMessages int
	MaxOutstandingBytes    int
}

func (p *PubsubConfig) InitClient(ctx context.Context, projectId string) (err error) {
	if p.Enabled {
		p.Client, err = pubsub.NewClient(ctx, projectId)
	}
	return err
}
