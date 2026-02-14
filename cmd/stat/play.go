package main

import (
	"context"
	"database/sql"

	"github.com/pdcgo/worker_stat/batch_compute"
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

		schema := "test"
		disableTemporary := true

		// schema := "stats"
		// disableTemporary := false

		graph := batch_compute.NewGraphContext(schema, disableTemporary)

		err = graph.Compute(ctx, tx,

			stock.DailyTeamAdjustmentCreated{},
			stock.TeamDailyStock{},
			stock.TeamStockErr{},
		)

		// err = graph.Compute(ctx, tx,
		// 	stock.DailyTeamOrderSpent{},
		// 	stock.DailyTeamBrokenCreated{},

		// 	stock.DailyTeamRestock{},
		// 	stock.TeamRestockState{},
		// 	stock.DailyTeamReturn{},

		// 	product.VariantSold{},
		// 	product.VariantCurrentStock{},

		// 	order.UserRevenueCreated{},
		// 	order.TeamHoldErr{},
		// 	order.ShopHoldErr{},
		// )

		if err != nil {
			tx.Rollback()
			return err
		}

		return nil
	}
}
