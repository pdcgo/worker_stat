package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"time"

	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/pdcgo/worker_stat/batch_metric/incidents/stock_overflow"
	"github.com/pdcgo/worker_stat/batch_metric/order"
	"github.com/pdcgo/worker_stat/batch_metric/product"
	"github.com/pdcgo/worker_stat/batch_metric/stock"
	"github.com/urfave/cli/v3"
	"gorm.io/gorm"
)

type BatchFunc cli.ActionFunc

func NewBatch(db *gorm.DB) BatchFunc {
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

		// getting schema
		var schema string = c.String("schema")

		// getting disableTemporary
		var disableTemporary bool = c.Bool("disable-temporary")

		graph := batch_compute.NewGraphContext(schema, disableTemporary, &filter)

		var tableToCompute []batch_compute.Table

		if c.Bool("compute-test") {
			// tableToCompute = []batch_compute.Table{
			// 	performance.DailyWarehousePicking{},
			// 	performance.DailyUserPicking{},
			// 	performance.DailyWarehouseCompleted{},
			// }

			tableToCompute = []batch_compute.Table{
				stock.DailyTeamOrderSpent{},
			}

		} else {
			tableToCompute = []batch_compute.Table{
				stock_overflow.OverflowHaveStock{},
				stock_overflow.OverflowDonthaveStock{},
				stock.InboundSpentNegative{},
				stock.SkuReadyStockErr{},

				stock.TeamStockErr{},

				stock.DailyTeamOrderSpent{},
				stock.DailyTeamBrokenCreated{},

				stock.DailyTeamRestock{},
				stock.TeamRestockState{},
				stock.DailyTeamReturn{},

				product.VariantSold{},
				product.VariantCurrentStock{},

				order.UserRevenueCreated{},
				order.TeamHoldErr{},
				order.ShopHoldErr{},
			}
		}

		err = graph.Compute(ctx, tx, tableToCompute...)
		if err != nil {
			tx.Rollback()
			return err
		}

		visual := c.String("visualization")
		if visual != "" {
			slog.Info("generate visualization", "path", visual)
			graph = batch_compute.NewGraphContext(schema, disableTemporary, &filter)
			f, err := os.OpenFile(visual, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
			if err != nil {
				return err
			}
			defer f.Close()
			err = graph.GenerateVisualization(f, tableToCompute...)
			if err != nil {
				return err
			}
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
