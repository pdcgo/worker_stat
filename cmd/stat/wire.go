//go:build wireinject
// +build wireinject

package main

import (
	"github.com/google/wire"
	"github.com/pdcgo/shared/configs"
	"github.com/urfave/cli/v3"
)

func InitializeAppReplication() (AppReplication, error) {
	wire.Build(
		configs.NewProductionConfig,
		NewProductionDatabase,
		NewPlayground,
		NewPlay,
		NewStockStream,
		NewStreaming,
		NewAppReplication,
	)
	return &cli.Command{}, nil
}
