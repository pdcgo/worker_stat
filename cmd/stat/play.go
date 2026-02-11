package main

import (
	"context"
	"database/sql"

	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/pdcgo/worker_stat/batch_metric/accounting"
	"github.com/pdcgo/worker_stat/batch_metric/product"
	"github.com/urfave/cli/v3"
	"gorm.io/gorm"
)

type PlayFunc cli.ActionFunc

func NewPlay(db *gorm.DB) PlayFunc {
	return func(ctx context.Context, c *cli.Command) error {
		var err error

		tx := db.
			Begin(&sql.TxOptions{
				Isolation: sql.LevelRepeatableRead,
			})

		defer tx.Commit()

		schema := "stats"

		err = batch_compute.
			NewCompute(
				tx,
				batch_compute.Schema(schema),
			).
			Compute(ctx,
				product.VariantSold{},
				product.VariantCurrentStock{},
				accounting.TeamReceivable{},
				accounting.ShopReceivable{},
				accounting.ShopReceivableErr{},
			)
		if err != nil {
			return err
		}
		return nil
	}
}
