package main

import (
	"context"

	"github.com/urfave/cli/v3"
	"github.com/wargasipil/stream_engine/stream_counter"
)

type DebugFunc cli.ActionFunc

func NewDebugFunc() DebugFunc {
	return func(ctx context.Context, c *cli.Command) error {
		cdata, err := stream_counter.NewOffsetCounter("/tmp/worker_stat/counter_data")
		if err != nil {
			return err
		}

		cdata.Debug(func() error {
			return nil
		})

		return nil
	}
}
