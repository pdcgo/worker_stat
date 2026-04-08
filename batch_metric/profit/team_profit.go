package profit

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/pdcgo/worker_stat/batch_metric/profit/cost"
)

type TeamDailyWithdrawal struct{}

// BuildQuery implements [batch_compute.Table].
func (t TeamDailyWithdrawal) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
		with d as (
			select 
				distinct
				wl.team_id,
				wl.shop_id,
				wl.amount,
				wl.at
			from v2_withdrawal_logs wl
		)

		select 
			date(d.at) as day,
			d.team_id as team_id,
			sum(d.amount) as amount
		from d
		group by (
			day,
			team_id
		)
		`
}

// TableName implements [batch_compute.Table].
func (t TeamDailyWithdrawal) TableName() string {
	return "team_daily_withdrawal"
}

// Temporary implements [batch_compute.Table].
func (t TeamDailyWithdrawal) Temporary() bool {
	return true
}

type TeamOrderRevenue struct{}

func (t TeamOrderRevenue) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			date(al.fund_at) as day,
			al.team_id,
			sum(al.amount) as total_revenue_amount,
			
			sum(al.real_revenue_amount) as real_revenue_amount,
			sum(al.estimated_revenue_amount) as estimated_revenue_amount,
			sum(al.order_cost_amount) as order_cost_amount,
			
			sum(al.adj_real_revenue_amount) as adj_real_revenue_amount,
			
			
			sum(al.additional_order_count) as additional_order_count,
			sum(al.additional_revenue_amount) as additional_revenue_amount
		from %s al
		group by (
			day,
			al.team_id
		)
		`,
		graph.DependName(t, AdjustmentLog{}),
	)
}

func (t TeamOrderRevenue) TableName() string {
	return "team_order_revenue"
}

func (t TeamOrderRevenue) Temporary() bool {
	return false
}

type TeamOrderRevenueInvalid struct{}

// BuildQuery implements [batch_compute.Table].
func (t TeamOrderRevenueInvalid) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with d as (
			select 
				coalesce(dw.day, tor.day) as day,
				coalesce(dw.team_id, tor.team_id) as team_id,
				coalesce(dw.amount, 0) as csv_amount,
				tor.total_revenue_amount
			from %s dw
			full join %s tor on tor.team_id = dw.team_id and tor.day = dw.day
		)

		select
			d.day,
			d.team_id,
			d.total_revenue_amount,
			d.csv_amount,
			(
				d.csv_amount + d.total_revenue_amount
			) as invalid_amount
		from d
		order by d.day desc
		`,
		graph.DependName(t, TeamDailyWithdrawal{}),
		graph.DependName(t, TeamOrderRevenue{}),
	)
}

// TableName implements [batch_compute.Table].
func (t TeamOrderRevenueInvalid) TableName() string {
	return "team_order_revenue_invalid"
}

// Temporary implements [batch_compute.Table].
func (t TeamOrderRevenueInvalid) Temporary() bool {
	return false
}

type TeamProfit struct{}

// BuildQuery implements [batch_compute.Table].
func (t TeamProfit) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 

			coalesce(tor.day, tac.day) as day,
			coalesce(tor.team_id, tac.team_id) as team_id,
			
			
			tor.real_revenue_amount,
			tac.ads_expense_amount,
			tor.order_cost_amount,
			(
				coalesce(tor.real_revenue_amount, 0)
				- coalesce(tac.ads_expense_amount, 0)
				- coalesce(tor.order_cost_amount, 0)
			) as selling_profit_amount
			
		from %s tor
		full join %s tac on tac.day = tor.day and tac.team_id = tor.team_id 
		`,
		graph.DependName(t, TeamOrderRevenue{}),
		graph.DependName(t, cost.TeamAdsCost{}),
	)
}

// TableName implements [batch_compute.Table].
func (t TeamProfit) TableName() string {
	return "team_profit"
}

// Temporary implements [batch_compute.Table].
func (t TeamProfit) Temporary() bool {
	return false
}
