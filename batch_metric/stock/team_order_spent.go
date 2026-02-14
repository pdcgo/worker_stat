package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type DailyTeamOrderSpent struct{}

// BuildQuery implements [batch_compute.Table].
func (d DailyTeamOrderSpent) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select
			cl.product_team_id as team_id,
			date(cl.created) as day,
			
			sum(cl.count) filter (where cl.tx_team_id = cl.product_team_id) as own_spent_count,
			sum(cl.total) filter (where cl.tx_team_id = cl.product_team_id) as own_spent_amount,
			
			sum(cl.count) filter (where cl.tx_team_id != cl.product_team_id) as cross_spent_count,
			sum(cl.total) filter (where cl.tx_team_id != cl.product_team_id) as cross_spent_amount,

			sum(cl.count) as total_spent_count,
			sum(cl.total) as total_spent_amount
			
		from %s cl
		join public.inv_transactions it on it.id = cl.tx_id
		where
			it.type = 'order'
			and it.created > '2025-09-09'

		group by (
			day,
			cl.product_team_id
		)
		`,
		graph.DependName(CreatedItemLog{}),
	)
}

// TableName implements [batch_compute.Table].
func (d DailyTeamOrderSpent) TableName() string {
	return "daily_team_order_spent"
}

// Temporary implements [batch_compute.Table].
func (d DailyTeamOrderSpent) Temporary() bool {
	return false
}
