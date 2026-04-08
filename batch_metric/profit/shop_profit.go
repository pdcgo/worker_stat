package profit

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/pdcgo/worker_stat/batch_metric/profit/cost"
)

type ShopDailyWithdrawal struct{}

func (s ShopDailyWithdrawal) BuildQuery(graph *batch_compute.GraphContext) string {
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
			d.shop_id as shop_id,
			sum(d.amount) as amount
		from d
		group by (
			day,
			shop_id
		)
	`
}

func (s ShopDailyWithdrawal) TableName() string {
	return "shop_daily_withdrawal"
}

func (s ShopDailyWithdrawal) Temporary() bool {
	return false
}

type ShopOrderRevenue struct{}

func (s ShopOrderRevenue) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			date(al.fund_at) as day,
			al.shop_id,
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
			al.shop_id
		)
		`,
		graph.DependName(s, AdjustmentLog{}),
	)
}

func (s ShopOrderRevenue) TableName() string {
	return "shop_order_revenue"
}

func (s ShopOrderRevenue) Temporary() bool {
	return false
}

type ShopOrderRevenueInvalid struct{}

// BuildQuery implements [batch_compute.Table].
func (s ShopOrderRevenueInvalid) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with d as (
			select 
				coalesce(dw.day, tor.day) as day,
				coalesce(dw.shop_id, tor.shop_id) as shop_id,
				coalesce(dw.amount, 0) as csv_amount,
				tor.total_revenue_amount
			from %s dw
			full join %s tor on tor.shop_id = dw.shop_id and tor.day = dw.day
		)

		select
			d.day,
			d.shop_id,
			d.total_revenue_amount,
			d.csv_amount,
			(
				d.csv_amount + d.total_revenue_amount
			) as invalid_amount
		from d
		order by d.day desc
		`,
		graph.DependName(s, ShopDailyWithdrawal{}),
		graph.DependName(s, ShopOrderRevenue{}),
	)
}

// TableName implements [batch_compute.Table].
func (s ShopOrderRevenueInvalid) TableName() string {
	return "shop_order_revenue_invalid"
}

// Temporary implements [batch_compute.Table].
func (s ShopOrderRevenueInvalid) Temporary() bool {
	return false
}

type ShopProfit struct{}

func (s ShopProfit) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			coalesce(sor.day, sac.day) as day,
			coalesce(sor.shop_id, sac.marketplace_id) as shop_id,
			
			sor.real_revenue_amount,
			sac.ads_expense_amount,
			sor.order_cost_amount,
			(
				coalesce(sor.real_revenue_amount, 0)
				- coalesce(sac.ads_expense_amount, 0)
				- coalesce(sor.order_cost_amount, 0)
			) as selling_profit_amount
			
		from %s sor
		full join %s sac on sac.day = sor.day and sac.marketplace_id = sor.shop_id 
		`,
		graph.DependName(s, ShopOrderRevenue{}),
		graph.DependName(s, cost.ShopAdsCost{}),
	)
}

func (s ShopProfit) TableName() string {
	return "shop_profit"
}

func (s ShopProfit) Temporary() bool {
	return false
}
