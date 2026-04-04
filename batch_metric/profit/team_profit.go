package profit

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/pdcgo/worker_stat/batch_metric/profit/cost"
)

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
