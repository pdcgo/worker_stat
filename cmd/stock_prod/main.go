package main

import (
	"context"
	"log"
	"os"

	"cloud.google.com/go/storage"
	"github.com/urfave/cli/v3"
	"github.com/wargasipil/stream_engine/stream_storage"
)

type ConfigStream struct {
	CounterDir string
}

func NewConfigStream() *ConfigStream {
	return &ConfigStream{
		CounterDir: "/tmp/stock_counter",
	}
}

func NewWalStream() *stream_storage.WalStream {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}

	return stream_storage.NewWalStream(
		ctx,
		"stream_experiment",
		"inventory_log",
		client,
	)
}

type Worker struct {
	Cli   *cli.Command
	Close func() error
}

func NewWorker(
	runStream RunStreamFunc,
) *Worker {
	return &Worker{
		Close: func() error {
			return nil
		},
		Cli: &cli.Command{
			Commands: []*cli.Command{
				{
					Name:        "run",
					Description: "running statistic stock",
					Action:      cli.ActionFunc(runStream),
				},
			},
		},
	}
}

func main() {
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
