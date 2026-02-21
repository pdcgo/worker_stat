package main

import (
	"context"
	"os"

	"github.com/urfave/cli/v3"
)

type AppReplication *cli.Command

func NewAppReplication(
	play PlayFunc,
	stockStream StockStreamFunc,
) AppReplication {
	return &cli.Command{
		Commands: []*cli.Command{
			{
				Name:        "play",
				Description: "batch processing playground",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "visualization",
						Aliases: []string{"v"},
						Usage:   "untuk generate visualization",
					},
				},
				Action: cli.ActionFunc(play),
			},
			{
				Name:        "stock_stream",
				Description: "streaming stock",
				Action:      cli.ActionFunc(stockStream),
			},
		},
	}
}

func main() {
	app, err := InitializeAppReplication()
	if err != nil {
		panic(err)
	}

	var cliApp *cli.Command = app
	err = cliApp.Run(context.Background(), os.Args)
	if err != nil {
		panic(err)
	}

}
