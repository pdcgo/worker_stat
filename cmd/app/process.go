package main

import (
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
