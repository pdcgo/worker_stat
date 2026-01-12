package main

import (
	"log"
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

func NewProcessHandler(kv stream_core.KeyStore) ProcessHandler {

	handler := stream_utils.NewChain(
		metric_daily.DailyCashFlowAccount(kv),
		metric_daily.DailyStockAccount(kv),
		metric_daily.DailyTeamToTeamAccountFunc(kv),
		metric_daily.DailyLiability(kv),
		metric_daily.CashFlow(kv),
		metric_team.TeamAccountFunc(kv),
	)

	return ProcessHandler(handler)
}

type PeriodicSnapshot func(t time.Time) error

func NewPeriodicSnapshot(kv stream_core.KeyStore) PeriodicSnapshot {

	// storage, err := stream_utils.NewFirestoreKeyStorage(context.Background(), "experimental")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// snapshotFunc := stream_utils.NewChainSnapshot(
	// 	storage.SnapshotHandler(),
	// )

	return func(t time.Time) error {
		var err error

		// writer := stream_utils.CsvWriter{Data: map[string][][]string{}}

		log.Println("----------------------- snapshot ----------------------------")
		// err = kv.Snapshot(t, snapshotFunc)
		log.Println("----------------------- end snapshot ------------------------")

		return err
	}
}
