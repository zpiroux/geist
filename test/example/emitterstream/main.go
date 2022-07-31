package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/zpiroux/geist"
)

// A Geist integration running a single autonomous stream, processing events from
// a simple custom source extractor, and logging transformed data with void sink.
// Run this test stream with go run .
// Graceful shutdown with Ctrl+C (or similar)
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go ensureGracefulShutdown(cancel)
	RunStream(ctx)
}

func ensureGracefulShutdown(cancel context.CancelFunc) {

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	log.Println("Initiating wait for shutdown (SIGINT/SIGTERM) signal")

	<-shutdown

	// Tell all downstream code/goroutines to cancel operations gracefully (via ctx cancel),
	log.Println("Received shutdown (SIGINT/SIGTERM) signal - initiating service cancellation.")
	cancel()
}

func RunStream(ctx context.Context) {
	var (
		g   *geist.Geist
		err error
	)

	config := geist.NewConfig()

	if err = config.RegisterExtractorType(NewEmitterFactory()); err != nil {
		log.Fatalf("geistConfig.RegisterExtractorType() error: %v", err)
	}

	if g, err = geist.New(ctx, config); err != nil {
		log.Fatalf("geist.New() error: %v", err)
	}

	go func() {
		streamId, err := g.RegisterStream(ctx, specEmitterStream)
		if err != nil {
			log.Fatalf("geist.RegisterStream() error: %v", err)
		}
		log.Printf("stream registered with streamId: %s", streamId)
	}()

	g.Run(ctx)
	g.Shutdown(ctx)
}

var specEmitterStream = []byte(`
    {
        "namespace": "my",
        "streamIdSuffix": "event-emitter-stream",
        "description": "Test stream continuously processing events from a custom extractor.",
        "version": 1,
        "source": {
            "type": "eventEmitter",
			"config": {
				"customConfig": {
					"emitIntervalSeconds": "2"
				}
			}
		},
        "transform": {
            "extractFields": [
                {
                    "fields": [
                        {
                            "id": "rawEvent"
                        },
                        {
                            "id": "eventTime",
							"jsonPath": "ts",
							"type": "unixTimestamp"
                        }
					]
                }
            ]
        },
        "sink": {
            "type": "void",
            "config": {
                "properties": [
                    {
                        "key": "logEventData",
                        "value": "true"
                    }
                ]
            }
        }
    }
`)
