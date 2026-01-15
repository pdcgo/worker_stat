package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/pdcgo/worker_stat/metric/metric_daily"
	"github.com/pdcgo/worker_stat/metric/metric_team"
	"github.com/pdcgo/worker_stat/writer"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
)

type PeriodicSnapshot func(ctx context.Context, dur time.Duration)

func NewPeriodicSnapshot(
	kv stream_core.KeyStore,
	statdb *StatDatabase,
) PeriodicSnapshot {

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

	// fungsi snapshot

	return func(ctx context.Context, dur time.Duration) {
		var err error
		last := time.Now()
		ti := time.NewTimer(dur)
		for {

			select {
			case <-ctx.Done():
				return
			case <-ti.C:
				err = kv.UpdatedKey(last, snapshotFunc)
				if err != nil {
					slog.Error(err.Error())
				}
				last = time.Now()
				ti.Reset(dur)
			}

		}
	}
}
