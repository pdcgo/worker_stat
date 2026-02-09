package main

import (
	"context"
	"database/sql"
	"time"

	"github.com/pdcgo/worker_stat/helpers"
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
			// PreparedDailyTable,
			// DailyShopHoldTable,
			// DailyTeamHoldTable,
			OrderCreatedLog,
			CurrentOrderHold,
			OrderCompletedLog,
			OrderHoldHistory,
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
		queries := []string{}
		for _, query := range queries {
			err = transaction.Exec(query).Error
			if err != nil {
				return err
			}
		}

		query := `
			select
				*
			from daily_team_holds
			where team_id = 31
			order by day asc
			`

		datas := []*DailyTeamOrderHoldHistory{}
		// datas := []map[string]any{}
		err = transaction.Raw(query).Find(&datas).Error
		if err != nil {
			return err
		}

		helpers.PrintTable(nil, datas)

		query = `
			select 
				*
			from created_team_order_holds
			where team_id = 31
			`

		raws := []*TeamHold{}
		// datas := []map[string]any{}
		err = transaction.Raw(query).Find(&raws).Error
		if err != nil {
			return err
		}

		helpers.PrintTable(nil, raws)

		// for _, data := range datas {
		// 	debugtool.LogJson(data)
		// }

		return err
	}
}

type TeamHold struct {
	TeamID        uint64
	TxCount       int64
	RevenueAmount float64
}

type DailyTeamOrderHoldHistory struct {
	Day                    time.Time
	TeamID                 uint64
	CreatedCount           int64
	CompletedCount         int64
	HoldCount              int64
	CreatedRevenueAmount   float64
	CompletedRevenueAmount float64
	HoldAmount             float64
}

func OrderHoldHistory(next processing.ChainNextFunc[*gorm.DB]) processing.ChainNextFunc[*gorm.DB] {
	return func(ctx context.Context, tx *gorm.DB) error {
		var err error

		queries := []string{
			`
			create temp table team_order_holds_log as
			select 
				COALESCE(cr.team_id, com.team_id) team_id,
				COALESCE(cr.day, com.day) as day,
				COALESCE(com.tx_count, 0) as completed_count,
				COALESCE(com.revenue_amount, 0) as completed_revenue_amount,
				COALESCE(cr.tx_count, 0) as created_count,
				COALESCE(cr.revenue_amount, 0) as created_revenue_amount
			from team_created_log cr
			full join team_completed_log com on 
				cr.team_id = com.team_id
				and cr.day = com.day
			`,
			`
			create temp table daily_team_holds as
			select 
				th.*,
				sum(th.created_count - th.completed_count) over (
					partition by th.team_id
					order by th.day asc) as hold_count,
				sum(th.created_revenue_amount - th.completed_revenue_amount) over (
					partition by th.team_id
					order by th.day asc) as hold_amount
			from team_order_holds_log th
			`,
		}

		for _, query := range queries {
			err = tx.Exec(query).Error
			if err != nil {
				return err
			}
		}

		return next(ctx, tx)
	}
}

func CurrentOrderHold(next processing.ChainNextFunc[*gorm.DB]) processing.ChainNextFunc[*gorm.DB] {
	return func(ctx context.Context, tx *gorm.DB) error {
		var err error

		queries := []string{
			`
			create temp table created_daily_order_holds as
			select 
				date(o.created_at) as day,
				o.team_id,
				o.order_mp_id as shop_id,
				o.created_by_id as user_id,
				count(id) as tx_count,
				sum(o.order_mp_total) as revenue_amount
				
			from orders o 
			where 
				o.status not in ('completed', 'return_problem', 'return_completed', 'cancel')
				and o.created_at > '2025-09-09'
				and o.is_partial != true
				and o.is_order_fake != true
			group by (
				day,
				team_id,
				shop_id,
				user_id
			)
			`,
			`
			create temp table created_team_order_holds as
			select 
				team_id,
				sum(tx_count) as tx_count,
				sum(revenue_amount) as revenue_amount
			from created_daily_order_holds
			group by team_id
			`,
		}

		for _, query := range queries {
			err = tx.Exec(query).Error
			if err != nil {
				return err
			}
		}

		return next(ctx, tx)
	}
}

func OrderCreatedLog(next processing.ChainNextFunc[*gorm.DB]) processing.ChainNextFunc[*gorm.DB] {
	return func(ctx context.Context, tx *gorm.DB) error {
		var err error
		queries := []string{
			`
			create temp table order_created_log as
			select 
				o.id as order_id,
				o.team_id as team_id,
				o.order_mp_id as shop_id,
				o.created_by_id as user_id,
				o.created_at as at,
				o.order_mp_total as revenue_amount
			from orders o
			where
				o.is_partial != true
				and o.is_order_fake != true
				and o.created_at > '2025-09-09'
			`,
			`
			create temp table team_created_log as
			select 
				team_id,
				date(at) as day,
				count(order_id) as tx_count,
				sum(revenue_amount) as revenue_amount
			from order_created_log
			group by (team_id, day)
			`,
		}

		for _, query := range queries {
			err = tx.Exec(query).Error
			if err != nil {
				return err
			}
		}

		return next(ctx, tx)
	}
}

func OrderCompletedLog(next processing.ChainNextFunc[*gorm.DB]) processing.ChainNextFunc[*gorm.DB] {
	return func(ctx context.Context, tx *gorm.DB) error {
		var err error
		queries := []string{
			`
			create temp table order_completed_log as
			with completed as (
				select 
					ot.order_id as order_id,
					
					min(ot.timestamp) as at,
					count(ot.id) as change_count,
					min(o.order_mp_total) as revenue_amount
				from order_timestamps ot
				join orders o on o.id = ot.order_id 
				where 
					ot.order_status in ('completed', 'return_problem', 'return', 'return_completed', 'cancel')
					and o.is_partial != true
					and o.is_order_fake != true
					and o.created_at > '2025-09-09'
				group by order_id
			)

			select 
				o.team_id,
				o.order_mp_id as shop_id,
				o.created_by_id as user_id,
				com.*
			from completed com
			join orders o on o.id = com.order_id
			`,
			// created team completed log
			`
			create temp table team_completed_log as
			select 
				team_id,
				date(at) as day,
				count(order_id) as tx_count,
				sum(revenue_amount) as revenue_amount
			from order_completed_log
			group by (team_id, day)
			`,
		}
		for _, query := range queries {
			err = tx.Exec(query).Error
			if err != nil {
				return err
			}
		}

		return next(ctx, tx)
	}
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
