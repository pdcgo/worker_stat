package main

import (
	"github.com/pdcgo/worker_stat/batch_model"
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

type ProcessHandler stream_utils.ChainNextHandler[*batch_model.BatchJournalEntry]

func NewProcessHandler(kv stream_core.KeyStore) ProcessHandler {

	handler := stream_utils.NewChain(

		metric_daily.DailyCashFlowAccount(kv),
		metric_daily.DailyStockAccount(kv),
		metric_daily.DailyTeamToTeamAccountFunc(kv),
		metric_daily.DailyLiability(kv),
		metric_daily.CashFlow(kv),
		metric_team.TeamAccountFunc(kv),
	)

	return func(next stream_utils.ChainNextFunc[*batch_model.BatchJournalEntry]) stream_utils.ChainNextFunc[*batch_model.BatchJournalEntry] {
		return func(batch *batch_model.BatchJournalEntry) error {
			for _, entry := range batch.Entries {
				err := handler(entry)
				if err != nil {
					return err
				}
				kv.PutUint64("accounting_pkey", uint64(entry.ID))
			}

			return next(batch)
		}
	}
}
