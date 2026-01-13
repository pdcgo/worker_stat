package main

import (
	"context"
	"log"
	"os"

	"github.com/urfave/cli/v3"
	"github.com/wargasipil/stream_engine/counter"
	"github.com/wargasipil/stream_engine/stream_core"
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
	kv := counter.NewKeyCounter("/tmp/worker_stat")
	return kv
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
