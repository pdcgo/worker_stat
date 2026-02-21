package main

import (
	"context"
	"log"
	"time"

	"github.com/urfave/cli/v3"
)

type StockStreamFunc cli.ActionFunc

func NewStockStream() StockStreamFunc {
	return func(ctx context.Context, c *cli.Command) error {

	Parent:
		for {
			select {
			case <-ctx.Done():
				break Parent
			default:

				log.Println("cikal bakal replikasi batch")
				time.Sleep(time.Second * 5)

			}
		}

		return nil
	}
}
