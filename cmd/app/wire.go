//go:build wireinject
// +build wireinject

package main

import (
	"github.com/google/wire"
	"github.com/pdcgo/shared/configs"
	"github.com/wargasipil/stream_engine/stream_core"
)

func InitializeWorker() (*Worker, error) {
	wire.Build(
		configs.NewProductionConfig,
		NewDatabase,
		NewStreamCoreConfig,
		stream_core.NewHashMapCounter,
		NewProcessHandler,
		NewPeriodicSnapshot,
		NewCalculate,
		NewSnapshotFunc,
		NewWorker,
	)
	return &Worker{}, nil

}
