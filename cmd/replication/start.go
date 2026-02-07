package main

import (
	"context"
	"log/slog"

	"github.com/dgraph-io/badger/v4"
	"github.com/pdcgo/shared/pkg/debugtool"
	"github.com/pdcgo/worker_stat/replication"
	"github.com/urfave/cli/v3"
)

type StartFunc cli.ActionFunc

func NewStartFunc(
	getReplication GetReplication,
	bdb *badger.DB,
) StartFunc {
	return func(ctx context.Context, c *cli.Command) error {

		rep, err := getReplication(ctx)
		if err != nil {
			return err
		}

		defer rep.Close(ctx)
		defer bdb.Close()

		// running backfill ?
		if c.Bool("backfill") {
			slog.Info("running backfill")
			commit, err := rep.RepeatableRead(ctx)
			if err != nil {
				return err
			}

			commit()
			return nil
		}

		// deduplication event
		handler := replication.NewChain(
			func(next replication.ChainNextFunc[*replication.ReplicationEvent]) replication.ChainNextFunc[*replication.ReplicationEvent] {
				return func(ctx context.Context, event *replication.ReplicationEvent) error { // filter order
					switch event.SourceMetadata.Table {
					case "order_timestamps":
						debugtool.LogJson(event)
					}

					return nil
				}
			},
		)

		err = rep.StreamStart(
			ctx,
			"devel_slot",
			"warehouse_publication",
			replication.ReplicationHandler(handler),
			// func(ctx context.Context, event *replication.ReplicationEvent) error {
			// 	switch event.SourceMetadata.Table {
			// 	case "orders":
			// 		debugtool.LogJson(event)
			// 	}

			// 	return nil
			// },
		)
		return err
	}
}
