package main

import (
	"context"
	"os"

	"github.com/urfave/cli/v3"
)

type AppReplication *cli.Command

func NewAppReplication(
	play PlayFunc,
) AppReplication {
	return &cli.Command{
		Commands: []*cli.Command{
			{
				Name:        "play",
				Description: "batch processing playground",
				Action:      cli.ActionFunc(play),
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
