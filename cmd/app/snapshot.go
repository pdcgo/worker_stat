package main

import (
	"context"
	"log"
	"reflect"
	"strings"
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
	kv *stream_core.HashMapCounter,
	statdb *StatDatabase,
	migrate Migrator,

) SnapshotFunc {
	log.Println("migrating database")
	err := migrate()
	if err != nil {
		log.Fatal(err)
	}

	pgwriterNext, pgwriterClose := writer.NewPostgresWriter(statdb.DB)
	defer pgwriterClose()

	pgwriter := stream_utils.NewChain(pgwriterNext)

	reportTeam := stream_utils.NewChainSnapshot(
		// func(next stream_utils.NextFunc) stream_utils.NextFunc {
		// 	return func(key string, kind reflect.Kind, value any) error {

		// 		if value == 0.00 {
		// 			return nil
		// 		}

		// 		if !strings.HasPrefix(key, "team") {
		// 			return next(key, kind, value)
		// 		}

		// 		if !strings.HasSuffix(key, "expense/balance") {
		// 			return next(key, kind, value)
		// 		}

		// 		if key == "team/101/expense/balance" {

		// 			diff := kv.GetFloat64("team/101/expense/balance") - kv.GetFloat64("team/101/liability/balance")
		// 			log.Printf("diff: %.3f\n", diff)

		// 			kv.PrintFloat64("team/101/asset/credit")
		// 			kv.PrintFloat64("team/101/asset/debit")
		// 			kv.PrintFloat64("team/101/liability/credit")
		// 			// log.Println(kv.KeyCollision("team/101/asset/credit"))

		// 		}

		// 		log.Printf("%s: %.3f\n", key, value)

		// 		return next(key, kind, value)
		// 	}
		// },
		func(next stream_utils.NextFunc) stream_utils.NextFunc {
			return func(key string, kind reflect.Kind, value any) error {
				if !strings.HasSuffix(key, "/error_balance") {
					return next(key, kind, value)

				}

				if !strings.HasPrefix(key, "daily/2026-01-06/team/70") {
					return next(key, kind, value)
				}

				// log.Printf("\n\n%s: %.3f\n", key, value)
				// kv.PrintFloat64("daily/2026-01-06/team/70/asset/balance")
				// kv.PrintFloat64("daily/2026-01-06/team/70/liability/balance")

				// log.Print("\n\n")

				// log.Printf("Expense: dif %.3f",
				// 	kv.PrintFloat64("daily/2026-01-06/team/70/expense/balance")-
				// 		kv.PrintFloat64("daily/2026-01-06/team/70/account/stock_borrow_cost/balance")-
				// 		kv.PrintFloat64("daily/2026-01-06/team/70/account/warehouse_cost/balance"),
				// )

				// log.Printf("Gros Profit: dif %.3f",
				// 	kv.PrintFloat64("daily/2026-01-06/team/70/revenue/balance")-
				// 		kv.PrintFloat64("daily/2026-01-06/team/70/expense/balance"),
				// )

				// kv.PrintFloat64("daily/2026-01-06/team/70/error_balance")
				// kv.PrintFloat64("daily/2026-01-06/team/70/balance")
				// kv.PrintFloat64("daily/2026-01-05/team/70/error_balance")
				// kv.PrintFloat64("daily/2026-01-05/team/70/balance")

				// log.Print("\n\n")

				return next(key, kind, value)
			}
		},
	)

	chainfunc := stream_utils.NewChainSnapshot(
		stream_utils.PararelChainSnapshot(
			reportTeam,
		),
		// func(next stream_utils.NextFunc) stream_utils.NextFunc {
		// 	return func(key string, kind reflect.Kind, value any) error {

		// 		if value == 0.00 {

		// 			return nil
		// 		}
		// 		if !strings.Contains(key, "payable_diff/") ||
		// 			!strings.Contains(key, "receivable_diff/") {
		// 			return nil
		// 		}

		// 		log.Printf("%s: %.3f\n", key, value)
		// 		return next(key, kind, value)
		// 	}
		// },
		func(next stream_utils.NextFunc) stream_utils.NextFunc {
			return func(key string, kind reflect.Kind, value any) error {
				// log.Println(key)

				if metric_team.IsMetricTeamAccount(key) {
					metric, err := metric_team.NewMetricTeamAccountFromKey(kv, key)
					if err != nil {
						return err
					}

					err = pgwriter(metric)
					if err != nil {
						return err
					}

					return next(key, kind, value)
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
					return next(key, kind, value)
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
					return next(key, kind, value)
				}

				// log.Println(key, value)
				return next(key, kind, value)
			}
		},
	)

	return func(ctx context.Context, c *cli.Command) error {
		// start := time.Now()

		t := time.Now().AddDate(-1, 0, 0)
		err := kv.Snapshot(t, false, chainfunc)
		if err != nil {
			return err
		}

		kv.PrintStat()

		return nil
	}
}
