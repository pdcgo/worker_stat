package profit

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/pdcgo/worker_stat/batch_metric/profit/cost"
)

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
