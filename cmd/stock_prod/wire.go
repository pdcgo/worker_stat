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
		NewConfigStream,
		NewDatabase,
		NewWalStream,
		NewRunStream,
		NewWorker,
	)
	return &Worker{}, nil

}
