package main

import (
	"context"
	"database/sql"
	"time"

	"github.com/pdcgo/shared/pkg/debugtool"
	"github.com/pdcgo/worker_stat/processing"
	"github.com/urfave/cli/v3"
	"gorm.io/gorm"
)

type BatchFunc cli.ActionFunc

func NewBatch(
	db *gorm.DB,
) BatchFunc {
	return func(ctx context.Context, c *cli.Command) error {
		var err error
		transaction := db.Begin(&sql.TxOptions{
			Isolation: sql.LevelRepeatableRead,
		})

		defer transaction.Commit()

		calculate := processing.NewChain(
			DoubleCompleteCorrection,
			PreparedDailyTable,
			DailyShopHoldTable,
			DailyTeamHoldTable,
		)

		err = calculate(ctx, transaction)
		if err != nil {
			return err
		}

		// query := `
		// 	select
		// 		*
		// 	from daily_team_holds dsh
		// 	where dsh.team_id = 31
		// 	order by dsh.day desc
		// 	limit 10
		// `

		query := `
			select 
				*
			from order_completed_ts dsh
			limit 10
		`

		// datas := []*DailyTeamHold{}
		datas := []map[string]any{}
		err = transaction.Raw(query).Find(&datas).Error
		if err != nil {
			return err
		}

		for _, data := range datas {
			debugtool.LogJson(data)
		}

		return err
	}
}

func DoubleCompleteCorrection(next processing.ChainNextFunc[*gorm.DB]) processing.ChainNextFunc[*gorm.DB] {
	return func(ctx context.Context, tx *gorm.DB) error {
		var err error
		query := `
		create temp table order_completed_ts as
		select 
			ot.order_id as order_id,
			min(ot.timestamp) as completed_at,
			count(ot.id) as change_count,
			min(o.order_mp_total) as revenue_amount
		from order_timestamps ot
		join orders o on o.id = ot.order_id 
		where 
			ot.order_status in ('completed', 'return_problem', 'return_completed')
			and o.created_at > '2025-09-09'
		group by order_id
		`

		err = tx.Exec(query).Error
		if err != nil {
			return err
		}

		return next(ctx, tx)
	}
}

type DailyTeamHold struct {
	Day                    time.Time
	TeamID                 uint64
	CreatedCount           int64
	CompletedCount         int64
	HoldCount              int64
	CreatedRevenueAmount   float64
	CompletedRevenueAmount float64
	HoldAmount             float64
}

func DailyTeamHoldTable(next processing.ChainNextFunc[*gorm.DB]) processing.ChainNextFunc[*gorm.DB] {
	return func(ctx context.Context, data *gorm.DB) error {
		var err error
		query := `
			create temp table daily_team_holds as
			with grouped as (
				select 
					pd.day,
					pd.team_id,
					sum(created_count) as created_count,
					sum(created_revenue_amount) as created_revenue_amount,
					sum(completed_count) as completed_count,
					sum(completed_revenue_amount) as completed_revenue_amount
				from prepared_daily pd
				group by (
					pd.day,
					pd.team_id
				)
			)
			select 
				*,
				sum(dsh.created_count - dsh.completed_count) over (
					partition by dsh.team_id
					order by dsh.day desc
				) as hold_count,
				sum(dsh.created_revenue_amount - dsh.completed_revenue_amount) over (
					partition by dsh.team_id
					order by dsh.day desc
				) as hold_amount
			from grouped dsh
		`

		err = data.Exec(query).Error
		if err != nil {
			return err
		}

		return next(ctx, data)
	}
}

type DailyShopHold struct {
	Day                    time.Time
	ShopID                 uint64
	CreatedCount           int64
	CompletedCount         int64
	CreatedRevenueAmount   float64
	CompletedRevenueAmount float64
	HoldCount              int64
	HoldAmount             float64
}

func DailyShopHoldTable(next processing.ChainNextFunc[*gorm.DB]) processing.ChainNextFunc[*gorm.DB] {
	return func(ctx context.Context, data *gorm.DB) error {
		var err error
		query := `
			create temp table daily_shop_holds as
			with grouped as (
				select 
					pd.day,
					pd.shop_id,
					sum(created_count) as created_count,
					sum(created_revenue_amount) as created_revenue_amount,
					sum(completed_count) as completed_count,
					sum(completed_revenue_amount) as completed_revenue_amount
				from prepared_daily pd
				group by (
					pd.day,
					pd.shop_id
				)
			)
			select 
				*,
				sum(dsh.created_revenue_amount - dsh.completed_revenue_amount) over (
					partition by dsh.shop_id
					order by dsh.day desc
				) as hold_amount
			from grouped dsh
		`

		err = data.Exec(query).Error
		if err != nil {
			return err
		}

		return next(ctx, data)
	}
}

type PreparedDaily struct {
	Day                    time.Time
	TeamID                 uint64
	ShopID                 uint64
	CreatedByID            uint64
	CompletedByID          uint64
	CreatedCount           int64
	CompletedCount         int64
	CreatedRevenueAmount   float64
	CompletedRevenueAmount float64
}

func PreparedDailyTable(next processing.ChainNextFunc[*gorm.DB]) processing.ChainNextFunc[*gorm.DB] {
	return func(ctx context.Context, tx *gorm.DB) error {
		var err error
		query := `
			create temp table prepared_daily as
			with logOrder as (
				select 
					ot.timestamp::date as day,
					o.team_id,
					o.order_mp_id as shop_id,
					o.created_by_id,
					ot.order_id,
					
					ot.order_status as status,
					o.order_mp_total as revenue_amount
					
				from order_timestamps ot 
				join orders o on o.id = ot.order_id
				where 
					ot.order_status in ('completed', 'return_problem', 'return_completed', 'created', 'cancel')
					and o.created_at > '2025-09-09'
			)

			select 
				
				lo.day,
				lo.team_id,
				lo.shop_id,
				lo.created_by_id,

				count(lo.order_id) filter (where lo.status = 'created') as created_count,
				sum(lo.revenue_amount) filter (where lo.status = 'created') as created_revenue_amount,
				count(lo.order_id) filter (where lo.status in ('completed', 'return_problem', 'return_completed', 'cancel')) as completed_count,
				sum(lo.revenue_amount) filter (where lo.status in ('completed', 'return_problem', 'return_completed', 'cancel')) as completed_revenue_amount
				
			from logOrder lo
			group by (
				lo.day,
				lo.team_id,
				lo.shop_id,
				lo.created_by_id
			)
		`

		err = tx.Exec(query).Error
		if err != nil {
			return err
		}

		return next(ctx, tx)
	}
}
