//go:build wireinject
// +build wireinject

package main

import (
	"github.com/google/wire"
	"github.com/pdcgo/shared/configs"
)

func InitializeWorker() (*Worker, error) {
	wire.Build(
		configs.NewProductionConfig,
		NewDatabase,
		NewStatDatabase,
		NewMigrator,
		NewStreamCoreConfig,
		NewKeystore,
		NewProcessHandler,
		NewPeriodicSnapshot,
		NewCalculateBalanceHistory,
		NewCalculate,
		NewSnapshotFunc,
		NewDebugFunc,
		NewWorker,
	)
	return &Worker{}, nil

}
