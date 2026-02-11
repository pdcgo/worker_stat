package main

import (
	"context"
	"os"

	"github.com/urfave/cli/v3"
)

type AppReplication *cli.Command

func NewAppReplication(
	batch BatchFunc,
) AppReplication {
	return &cli.Command{
		Commands: []*cli.Command{
			{
				Name:        "batch",
				Description: "batch processing",
				Action:      cli.ActionFunc(batch),
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
