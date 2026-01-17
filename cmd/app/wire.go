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
		NewStorageClient,
		NewMigrator,
		// bagian keyvalue
		NewDeleteStateFunc,
		NewStreamCoreConfig,
		NewKeystore,
		NewWalStream,
		NewProcessHandler,
		NewPeriodicSnapshot,
		NewCalculate,
		NewSnapshotFunc,
		NewDebugFunc,
		NewWorker,
	)
	return &Worker{}, nil

}
