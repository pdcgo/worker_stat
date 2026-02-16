package main

import (
	"context"
	"database/sql"
	"os"
	"time"

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

		startDate, err := time.Parse("2006-01-02", "2025-09-09")

		filter := batch_compute.GlobalFilter{
			StartDate: startDate,
		}

		schema := "test"
		disableTemporary := true

		// schema := "stats"
		// disableTemporary := false

		graph := batch_compute.NewGraphContext(schema, disableTemporary, &filter)

		tableToCompute := []batch_compute.Table{
			stock.InboundSpentNegative{},
			stock.SkuReadyStockErr{},
		}

		err = graph.Compute(ctx, tx, tableToCompute...)
		if err != nil {
			tx.Rollback()
			return err
		}

		graph = batch_compute.NewGraphContext(schema, disableTemporary, &filter)
		f, err := os.OpenFile("visualization.mmd", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		defer f.Close()
		err = graph.GenerateVisualization(f, tableToCompute...)
		if err != nil {
			return err
		}

		// err = graph.Compute(ctx, tx,
		// 	stock.InboundSpentNegative{},
		// )

		// err = graph.Compute(ctx, tx,
		// 	stock.TeamStockErr{},

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

		return nil
	}
}
