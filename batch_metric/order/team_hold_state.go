package order

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type TeamHoldState struct{}

// BuildQuery implements [batch_compute.Table].
func (t TeamHoldState) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
	select 
		coh.team_id,
		sum(coh.tx_count) as tx_count,
		sum(coh.revenue_amount) as revenue_amount
	from %s coh
	group by coh.team_id
	`,
		graph.DependName(t, CurrentOrderHold{}),
	)
}

// TableName implements batch_compute.Table.
func (t TeamHoldState) TableName() string {
	return "team_hold_state"
}

// Temporary implements batch_compute.Table.
func (t TeamHoldState) Temporary() bool {
	return false
}
