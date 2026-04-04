package profit

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/pdcgo/worker_stat/batch_metric/profit/cost"
)

type UserOrderRevenue struct{}

// BuildQuery implements [batch_compute.Table].
func (u UserOrderRevenue) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			date(al.fund_at) as day,
			al.user_id,
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
			al.user_id
		)
		`,
		graph.DependName(u, AdjustmentLog{}),
	)
}

// TableName implements [batch_compute.Table].
func (u UserOrderRevenue) TableName() string {
	return "user_order_revenue"
}

// Temporary implements [batch_compute.Table].
func (u UserOrderRevenue) Temporary() bool {
	return false
}

type UserProfit struct{}

func (u UserProfit) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			coalesce(uor.day, uac.day) as day,
			coalesce(uor.user_id, uac.user_id) as user_id,
			
			uor.real_revenue_amount,
			uac.ads_expense_amount,
			uor.order_cost_amount,
			(
				coalesce(uor.real_revenue_amount, 0)
				- coalesce(uac.ads_expense_amount, 0)
				- coalesce(uor.order_cost_amount, 0)
			) as selling_profit_amount
			
		from %s uor
		full join %s uac on uac.day = uor.day and uac.user_id = uor.user_id 
		`,
		graph.DependName(u, UserOrderRevenue{}),
		graph.DependName(u, cost.UserAdsCost{}),
	)
}

func (u UserProfit) TableName() string {
	return "user_profit"
}

func (u UserProfit) Temporary() bool {
	return false
}
