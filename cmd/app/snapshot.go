package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/pdcgo/worker_stat/metric/metric_key"
	"github.com/urfave/cli/v3"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
)

type SnapshotFunc cli.ActionFunc

func NewSnapshotFunc(
	kv *stream_core.HashMapCounter,
) SnapshotFunc {

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
		func(next stream_utils.NextFunc) stream_utils.NextFunc {
			return func(key string, kind reflect.Kind, value any) error {

				if value == 0.00 {

					return nil
				}
				if !strings.Contains(key, "payable_diff/") ||
					!strings.Contains(key, "receivable_diff/") {
					return nil
				}

				log.Printf("%s: %.3f\n", key, value)
				return next(key, kind, value)
			}
		},
	)

	// storage, err := stream_utils.NewFirestoreKeyStorage(context.Background(), "experimental")
	// if err != nil {
	// 	panic(err)
	// }
	return func(ctx context.Context, c *cli.Command) error {
		// start := time.Now()

		t := time.Now().AddDate(-1, 0, 0)
		err := kv.Snapshot(t, false, chainfunc)
		if err != nil {
			return err
		}

		// -------------------playground here
		fmt.Printf("\n\n")

		prefix := metric_key.NewDailyTeamPrefix(time.Now(), 70)
		errprefix := metric_key.NewErrorPrefix(prefix)

		kv.PrintFloat64(prefix.Join("all_cash_account/balance"))
		kv.PrintFloat64(prefix.Join("all_cash_account/debit"))
		kv.PrintFloat64(prefix.Join("all_cash_account/credit"))
		fmt.Printf("\n\n")

		fmt.Printf("%s: %.3f", errprefix.Join("all_cash"),
			kv.GetFloat64(prefix.Join("all_cash_account/credit"))-
				kv.PrintFloat64(prefix.Join("account/ads_expense/debit"))-
				kv.PrintFloat64(prefix.Join("account/packing_cost/debit"))-
				kv.PrintFloat64(prefix.Join("account/bank_fee/debit"))-
				kv.PrintFloat64(prefix.Join("account/stock_borrow_cost/debit"))-
				kv.PrintFloat64(prefix.Join("account/stock_pending/debit"))-
				kv.PrintFloat64(prefix.Join("account/warehouse_cost/debit")),
		)
		fmt.Printf("\n\n")

		kv.PrintFloat64(prefix.Join("all_stock/balance"))
		kv.PrintFloat64(prefix.Join("all_stock/debit"))
		kv.PrintFloat64(prefix.Join("all_stock/credit"))
		fmt.Printf("\n\n")

		kv.PrintFloat64(prefix.Join("account/cash/balance"))
		kv.PrintFloat64(prefix.Join("cash/last_balance"))
		kv.PrintFloat64(prefix.Join("account/shopeepay/balance"))
		kv.PrintFloat64(prefix.Join("shopeepay/last_balance"))

		fmt.Printf("\n\n")

		// // change to firestore
		// err = kv.Snapshot(start, true, stream_utils.NewChainSnapshot(storage.SnapshotHandler()))
		// if err != nil {
		// 	return err
		// }

		kv.PrintStat()

		return nil
	}
}
