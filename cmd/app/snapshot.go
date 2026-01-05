package main

import (
	"context"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/urfave/cli/v3"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
)

type SnapshotFunc cli.ActionFunc

func NewSnapshotFunc(
	kv *stream_core.HashMapCounter,
) SnapshotFunc {

	reportTeam := stream_utils.NewChainSnapshot(
		func(next stream_utils.NextFunc) stream_utils.NextFunc {
			return func(key string, kind reflect.Kind, value any) error {

				if value == 0.00 {
					return nil
				}

				if !strings.Contains(key, "asset") {
					return nil
				}

				// log.Printf("%s: %.3f\n", key, value)

				return next(key, kind, value)
			}
		},
	)

	chainfunc := stream_utils.NewChainSnapshot(
		stream_utils.PararelChainSnapshot(
			reportTeam,
			func(key string, kind reflect.Kind, value any) error {
				if !strings.Contains(key, "cash/") {
					return nil
				}

				if !strings.Contains(key, "balance") {
					return nil
				}

				if value == 0.00 {
					return nil
				}
				log.Printf("%s: %.3f\n", key, value)

				return nil
			},
			func(key string, kind reflect.Kind, value any) error {

				if !strings.Contains(key, "expense") {
					return nil
				}

				if value == 0.00 {
					return nil
				}
				log.Printf("%s: %.3f\n", key, value)

				return nil
			},
		),
		func(next stream_utils.NextFunc) stream_utils.NextFunc {
			return func(key string, kind reflect.Kind, value any) error {
				err := reportTeam(key, kind, value)
				if err != nil {
					return err
				}

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

	return func(ctx context.Context, c *cli.Command) error {
		t := time.Now().AddDate(-1, 0, 0)
		err := kv.Snapshot(t, chainfunc)
		if err != nil {
			return err
		}

		diff := kv.GetFloat64("daily/2026-01-05/team/47/error/payable_diff/team/31/amount")
		receivable := kv.GetFloat64("daily/2026-01-05/team/47/account/payable/team/31/balance")
		payable := kv.GetFloat64("daily/2026-01-05/team/31/account/receivable/team/47/balance")

		log.Println(kv.KeyCollision("daily/2026-01-05/team/47/error/payable_diff/team/31/amount"))

		log.Printf("diff: %.3f payable: %.3f receivable: %.3f\n", diff, payable, receivable)

		kv.Merge(stream_core.MergeOpAdd, reflect.Float64, "daily/2026-01-05/team/47/error/payable_diff/team/31/amount",
			"daily/2026-01-05/team/47/account/payable/team/31/balance",
			"daily/2026-01-05/team/31/account/receivable/team/47/balance",
		)

		diff = kv.GetFloat64("daily/2026-01-05/team/47/error/payable_diff/team/31/amount")
		receivable = kv.GetFloat64("daily/2026-01-05/team/47/account/payable/team/31/balance")
		payable = kv.GetFloat64("daily/2026-01-05/team/31/account/receivable/team/47/balance")

		log.Println(kv.KeyCollision("daily/2026-01-05/team/47/error/payable_diff/team/31/amount"))

		log.Printf("diff: %.3f payable: %.3f receivable: %.3f\n", diff, payable, receivable)
		log.Printf("all Asset: %.3f", kv.GetFloat64("all/asset"))
		log.Printf("all Liability: %.3f", kv.GetFloat64("all/liability"))
		log.Printf("all Revenue: %.3f", kv.GetFloat64("all/revenue"))
		log.Printf("all Expense: %.3f", kv.GetFloat64("all/expense"))
		return nil
	}
}
