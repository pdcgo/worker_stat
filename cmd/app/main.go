package main

import (
	"context"
	"log"
	"os"

	"cloud.google.com/go/storage"
	"github.com/urfave/cli/v3"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_counter"
	"github.com/wargasipil/stream_engine/stream_storage"
	"gorm.io/gorm"
)

func NewStreamCoreConfig() *stream_core.CoreConfig {
	return &stream_core.CoreConfig{
		HashMapCounterPath:  "/tmp/worker_stat/counter",
		HashMapCounterSlots: 134_217_728,
		DynamicValuePath:    "/tmp/worker_stat/value",
	}
}

func NewKeystore() stream_core.KeyStore {
	kv := stream_counter.NewKeyCounter("/tmp/worker_stat")
	return kv
}

func NewStorageClient() *storage.Client {
	client, err := storage.NewClient(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func NewWalStream(
	client *storage.Client,
) *stream_storage.WalStream {
	wal := stream_storage.NewWalStream(
		context.Background(),
		"stream_experiment",
		"journal_entries",
		client,
	)

	return wal

}

type Worker struct {
	Cli   *cli.Command
	Close func() error
}

func NewWorker(
	db *gorm.DB,
	kv stream_core.KeyStore,
	calculate CalculateFunc,
	snapshot SnapshotFunc,
	debugfunc DebugFunc,
	databaseDelete DeleteStateFunc,
) *Worker {

	return &Worker{
		Cli: &cli.Command{
			Commands: []*cli.Command{
				{
					Name:        "calculate",
					Description: "running calculate",
					Action:      cli.ActionFunc(calculate),
				},
				{
					Name:        "snapshot",
					Description: "running snapshot",
					Action:      cli.ActionFunc(snapshot),
				},
				{
					Name:        "debug",
					Description: "running debug",
					Action:      cli.ActionFunc(debugfunc),
				},
				{
					Name:        "database",
					Description: "running database ops",
					Commands: []*cli.Command{
						{
							Name:        "delete",
							Description: "deleting database",
							Action:      cli.ActionFunc(databaseDelete),
						},
					},
				},
			},
		},

		Close: func() error {
			return kv.Close()
		},
	}

}

func main() {

	os.MkdirAll("/tmp/worker_stat", os.ModeDir)

	worker, err := InitializeWorker()
	if err != nil {
		log.Fatal(err)
	}

	defer worker.Close()

	err = worker.Cli.Run(context.Background(), os.Args)
	if err != nil {
		panic(err)
	}

}
