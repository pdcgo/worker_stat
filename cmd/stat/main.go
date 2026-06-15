package main

import (
	"context"
	"os"

	"github.com/pdcgo/shared/custom_connect"
	"github.com/pdcgo/shared/pkg/cloud_logging"
	"github.com/urfave/cli/v3"
)

type AppReplication *cli.Command
type DefaultFlag []cli.Flag

func (d DefaultFlag) With(flags ...cli.Flag) []cli.Flag {
	res := []cli.Flag{}
	res = append(res, d...)
	res = append(res, flags...)
	return res
}

func NewAppReplication(
	batch BatchFunc,
	playground PlaygroundFunc,
	stockStream StockStreamFunc,
	// streamPlayground StreamingFunc,
) AppReplication {

	defaultFlag := DefaultFlag{
		&cli.BoolFlag{
			Name:    "debug",
			Aliases: []string{"d"},
			Usage:   "untuk logger mode debug",
		},
		&cli.BoolFlag{
			Name:    "disable-temporary",
			Aliases: []string{"dt"},
			Usage:   "disable temporary table",
		},
		&cli.StringFlag{
			Name:    "schema",
			Aliases: []string{"s"},
			Usage:   "schema",
			Value:   "test",
		},
		&cli.StringFlag{
			Name:    "visualization",
			Aliases: []string{"v"},
			Usage:   "untuk generate visualization",
		},
	}

	return &cli.Command{
		Commands: []*cli.Command{
			{
				Name:        "batch",
				Description: "batch processing playground",
				Flags: defaultFlag.
					With(

						&cli.BoolFlag{
							Name:    "compute-test",
							Aliases: []string{"ct"},
							Usage:   "compute test",
						},
					),
				Action: cli.ActionFunc(
					WithTelemetry(
						"worker/worker_stat/batch",
						cli.ActionFunc(batch),
					),
				),
			},
			{
				Name:        "stock_stream",
				Description: "streaming stock",
				Action:      cli.ActionFunc(stockStream),
			},
			// {
			// 	Name:        "streaming",
			// 	Description: "streaming playground",
			// 	Flags:       defaultFlag,
			// 	Action:      cli.ActionFunc(streamPlayground),
			// },
			{
				Name:        "playground",
				Description: "playground testing",
				Action:      cli.ActionFunc(playground),
			},
		},
	}
}

func main() {
	if os.Getenv("DISABLE_CLOUD_LOGGING") == "" {
		cloud_logging.SetCloudLoggingDefault()
	}
	ctx := context.Background()
	cancelTtracer, err := custom_connect.InitTracer("worker-stat")
	if err != nil {
		panic(err)
	}
	defer cancelTtracer(ctx)

	app, err := InitializeAppReplication()
	if err != nil {
		cancelTtracer(ctx)
		panic(err)
	}

	var cliApp *cli.Command = app
	err = cliApp.Run(ctx, os.Args)
	if err != nil {
		cancelTtracer(ctx)
		panic(err)
	}

}
