package main

import (
	"context"
	"database/sql"

	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/pdcgo/worker_stat/batch_metric/order"
	"github.com/pdcgo/worker_stat/batch_metric/product"
	"github.com/pdcgo/worker_stat/batch_metric/stock"
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
		graph := batch_compute.NewGraphContext(batch_compute.Schema(schema))

		graph.Compute(ctx, tx,
			product.VariantSold{},
			product.VariantCurrentStock{},
			order.UserRevenueCreated{},

			order.TeamHoldErr{},
			order.ShopHoldErr{},
			stock.DailyTeamRestock{},
		)

		err = batch_compute.
			NewCompute(
				tx,
				batch_compute.Schema(schema),
			).
			Compute(ctx,
				product.VariantSold{},
				product.VariantCurrentStock{},
				// accounting.TeamReceivable{},
				// accounting.ShopReceivable{},
				// accounting.ShopReceivableErr{},
				order.UserRevenueCreated{},
				// untuk shop
				order.DailyShopHold{},
				order.ShopHoldState{},
				order.ShopHoldErr{},
				// untuk team
				order.DailyTeamHold{},
				order.TeamHoldState{},
				order.TeamHoldErr{},
				// untuk stock
				stock.DailyTeamRestock{},
			)
		if err != nil {
			tx.Rollback()
			return err
		}
		return nil
	}
}
