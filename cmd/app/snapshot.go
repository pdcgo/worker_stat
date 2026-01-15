package main

import (
	"context"
	"log"
	"time"

	"github.com/pdcgo/worker_stat/metric/metric_daily"
	"github.com/pdcgo/worker_stat/metric/metric_team"
	"github.com/pdcgo/worker_stat/writer"
	"github.com/urfave/cli/v3"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
)

type SnapshotFunc cli.ActionFunc

func NewSnapshotFunc(
	kv stream_core.KeyStore,
	statdb *StatDatabase,
	migrate Migrator,

) SnapshotFunc {
	log.Println("migrating database")
	err := migrate()
	if err != nil {
		log.Fatal(err)
	}

	pgwriterNext, pgwriterClose := writer.NewPostgresWriter(context.Background(), statdb.DB, time.Minute)
	defer pgwriterClose()

	pgwriter := stream_utils.NewChain(pgwriterNext)

	snapshotFunc := stream_utils.NewChain(
		func(next stream_utils.ChainNextFunc[string]) stream_utils.ChainNextFunc[string] {
			return func(key string) error {
				if metric_team.IsMetricTeamAccount(key) {
					metric, err := metric_team.NewMetricTeamAccountFromKey(kv, key)
					if err != nil {
						return err
					}

					err = pgwriter(metric)
					if err != nil {
						return err
					}

					return next(key)
				}

				if metric_daily.IsMetricDailyTeamAccount(key) {
					metric, err := metric_daily.NewMetricDailyTeamAccountFromKey(kv, key)
					if err != nil {
						return err
					}

					err = pgwriter(metric)
					if err != nil {
						return err
					}
					return next(key)
				}

				if metric_daily.IsMetricDailyTeamToTeamAccount(key) {
					metric, err := metric_daily.NewMetricDailyTeamToTeamAccountFromKey(kv, key)
					if err != nil {
						return err
					}

					err = pgwriter(metric)
					if err != nil {
						return err
					}
					return next(key)
				}

				return next(key)

			}
		},
	)

	return func(ctx context.Context, c *cli.Command) error {
		// start := time.Now()

		kv.UpdatedKey(time.Now().AddDate(-1, 0, 0), snapshotFunc)

		// t := time.Now().AddDate(-1, 0, 0)
		// err := kv.Snapshot(t, false, chainfunc)
		// if err != nil {
		// 	return err
		// }

		// kv.PrintStat()

		return nil
	}
}
