package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type DailyTeamAdjustmentCreated struct{}

// BuildQuery implements [batch_compute.Table].
func (d DailyTeamAdjustmentCreated) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			date(c.created) as day,
			c.team_id as team_id,
			count(c.tx_id) as tx_count,
			sum(c.item_count) as item_count,
			sum(c.item_amount) as total_amount
			
		from %s c
		group by (
			day,
			team_id
		)
		`,
		graph.DependName(d, AdjustmentCreatedLog{}),
	)
}

// TableName implements [batch_compute.Table].
func (d DailyTeamAdjustmentCreated) TableName() string {
	return "daily_team_adjustment_created"
}

// Temporary implements [batch_compute.Table].
func (d DailyTeamAdjustmentCreated) Temporary() bool {
	return false
}
