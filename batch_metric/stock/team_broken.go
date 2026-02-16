package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type DailyTeamBrokenCreated struct{}

// BuildQuery implements [batch_compute.Table].
func (d DailyTeamBrokenCreated) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			date(c.created) as day,
			c.team_id as team_id,
			count(c.tx_id) as tx_count,
			sum(c.item_count) as item_count,
			sum(c.amount) as total_amount
			
		from %s c
		group by (
			day,
			team_id
		)
		`,
		graph.DependName(d, BrokenCreatedLog{}),
	)
}

// TableName implements [batch_compute.Table].
func (d DailyTeamBrokenCreated) TableName() string {
	return "daily_team_broken_created"
}

// Temporary implements [batch_compute.Table].
func (d DailyTeamBrokenCreated) Temporary() bool {
	return false
}
