package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/urfave/cli/v3"
	"gorm.io/gorm"
)

type BatchFunc cli.ActionFunc

func NewBatch(
	db *gorm.DB,
) BatchFunc {
	return func(ctx context.Context, c *cli.Command) error {
		var err error

		tx := db.
			Begin(&sql.TxOptions{
				Isolation: sql.LevelRepeatableRead,
			})

		defer tx.Commit()

		schema := "stats"

		log.Println("starting computing dana tertahan")

		orderCreatedLog := batch_compute.NewTableSelect(
			"order_created_log",
			`
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
			map[string]batch_compute.Table{},
		)

		teamCreatedLog := batch_compute.NewTableSelect(
			"team_created_log",
			`
			select 
				team_id,
				date(at) as day,
				count(order_id) as tx_count,
				sum(revenue_amount) as revenue_amount
			from {{.orderCreatedLogTable}}
			group by (team_id, day)
			`,
			map[string]batch_compute.Table{
				"orderCreatedLog": orderCreatedLog,
			},
		)

		orderCompletedLog := batch_compute.NewTableSelect(
			"order_completed_log",
			`
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
			map[string]batch_compute.Table{},
		)

		teamCompletedLog := batch_compute.NewTableSelect(
			"team_completed_log",
			`
			select 
				team_id,
				date(at) as day,
				count(order_id) as tx_count,
				sum(revenue_amount) as revenue_amount
			from {{.orderCompletedLogTable}}
			group by (team_id, day)
			`,
			map[string]batch_compute.Table{
				"orderCompletedLog": orderCompletedLog,
			},
		)

		teamOrderHoldLog := batch_compute.NewTableSelect(
			"team_order_holds_log",
			`
			select 
				COALESCE(cr.team_id, com.team_id) team_id,
				COALESCE(cr.day, com.day) as day,
				COALESCE(com.tx_count, 0) as completed_count,
				COALESCE(com.revenue_amount, 0) as completed_revenue_amount,
				COALESCE(cr.tx_count, 0) as created_count,
				COALESCE(cr.revenue_amount, 0) as created_revenue_amount
			from {{.teamCreatedLogTable}} cr
			full join {{.teamCompletedLogTable}} com on 
				cr.team_id = com.team_id
				and cr.day = com.day
			`,
			map[string]batch_compute.Table{
				"teamCreatedLog":   teamCreatedLog,
				"teamCompletedLog": teamCompletedLog,
			},
		)

		dailyTeamHold := batch_compute.NewTableSelect(
			"daily_team_holds",
			`
			select 
				th.*,
				sum(th.created_count - th.completed_count) over (
					partition by th.team_id
					order by th.day asc) as hold_count,
				sum(th.created_revenue_amount - th.completed_revenue_amount) over (
					partition by th.team_id
					order by th.day asc) as hold_amount
			from {{.teamOrderHoldLogTable}} th
			`,
			map[string]batch_compute.Table{
				"teamOrderHoldLog": teamOrderHoldLog,
			},
		)

		lastTeamHold := batch_compute.NewTableSelect(
			"last_daily_team_holds",
			`
			select 
				dsh.*
			from {{.dailyTeamHoldTable}} dsh
			join (
				select
					max(day) as day,
					team_id
				from {{.dailyTeamHoldTable}} dsh
				group by team_id
			) l on l.day = dsh.day and l.team_id = dsh.team_id
			`,
			map[string]batch_compute.Table{
				"dailyTeamHold": dailyTeamHold,
			},
		)

		createdOrderHold := batch_compute.NewTableSelect(
			"created_daily_order_holds",
			`
			select 
				date(o.created_at) as day,
				o.team_id,
				o.order_mp_id as shop_id,
				o.created_by_id as user_id,
				count(id) as tx_count,
				sum(o.order_mp_total) as revenue_amount
				
			from orders o 
			where 
				o.status not in ('completed', 'return_problem', 'return_completed', 'return', 'problem', 'cancel')
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
			map[string]batch_compute.Table{},
		)

		teamHold := batch_compute.NewTableSelect(
			"created_team_order_holds",
			`
			select 
				team_id,
				sum(tx_count) as tx_count,
				sum(revenue_amount) as revenue_amount
			from {{.orderHoldTable}}
			group by team_id
			`,
			map[string]batch_compute.Table{
				"orderHold": createdOrderHold,
			},
		)
		crossCheckTeamHoldLast := batch_compute.NewTableSelect(
			"team_hold_err",
			`
			select
				l.team_id,
				(l.hold_count - c.tx_count) as hold_count_err,
				(l.hold_amount - c.revenue_amount) as hold_amount_err
				
			from {{.lastTeamHoldTable}} l
			left join {{.teamHoldTable}} c
				on l.team_id = c.team_id
			`,
			map[string]batch_compute.Table{
				"teamHold":     teamHold,
				"lastTeamHold": lastTeamHold,
			},
		)

		err = batch_compute.
			NewCompute(
				tx,
				schema,
			).
			Compute(ctx,
				crossCheckTeamHoldLast,
			)
		if err != nil {
			return err
		}

		return err
	}
}
