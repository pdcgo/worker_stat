package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/pdcgo/worker_stat/replication"
	"github.com/urfave/cli/v3"
)

type BackfillFunc cli.ActionFunc

func NewBackfillFunc(
	getReplication GetReplication,
	state replication.ReplicationState,
) BackfillFunc {
	return func(ctx context.Context, c *cli.Command) error {
		slog.Info("start backfilling data")
		rep, err := getReplication(ctx)
		if err != nil {
			return err
		}
		defer rep.Close(ctx)

		// err = rep.BeginRepeatableRead(ctx)
		// if err != nil {
		// 	return err
		// }

		lsn, err := rep.GetLsn(ctx)
		if err != nil {
			return err
		}

		slog.Info("current sln", "sln", lsn)

		time.Sleep(time.Second * 15)

		return nil
	}
}
