package main

import (
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/pdcgo/worker_stat/metric/metric_daily"
	"github.com/pdcgo/worker_stat/metric/metric_team"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
	"gorm.io/gorm"
)

type Table struct {
	Name string
}

type PostgresWriter struct {
	db *gorm.DB
}

// -------------------------------------

type ProcessHandler func(entry *accounting_core.JournalEntry) error

func NewProcessHandler(kv *stream_core.HashMapCounter) ProcessHandler {

	handler := stream_utils.NewChain(
		metric_daily.DailyTeamAccount(kv),
		metric_daily.DailyLiability(kv),
		metric_daily.CashFlow(kv),
		metric_team.TeamAccount(kv),
	)

	return ProcessHandler(handler)
}

type PeriodicSnapshot func(t time.Time) error

func NewPeriodicSnapshot(kv *stream_core.HashMapCounter) PeriodicSnapshot {

	snapshotFunc := stream_utils.NewChainSnapshot(
		func(next stream_utils.NextFunc) stream_utils.NextFunc {
			return func(key string, kind reflect.Kind, value any) error {
				if value == 0 {
					return nil
				}
				if !strings.Contains(key, "payable_diff") {
					return nil
				}

				log.Printf("%s: %.3f\n", key, value)
				return next(key, kind, value)
			}
		},
	)

	return func(t time.Time) error {
		var err error

		// writer := stream_utils.CsvWriter{Data: map[string][][]string{}}

		log.Println("----------------------- snapshot ----------------------------")
		err = kv.Snapshot(t, snapshotFunc)
		log.Println("----------------------- end snapshot ------------------------")

		return err
	}
}
