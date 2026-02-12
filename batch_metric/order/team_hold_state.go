package order

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type TeamHoldState struct{}

// CreateQuery implements batch_compute.Table.
func (t TeamHoldState) CreateQuery(schema batch_compute.Schema) string {
	return fmt.Sprintf(
		`
	select 
		coh.team_id,
		sum(coh.tx_count) as tx_count,
		sum(coh.revenue_amount) as revenue_amount
	from %s coh
	group by coh.team_id
	`,
		schema.GetTableName(CurrentOrderHold{}),
	)
}

// DependsTable implements batch_compute.Table.
func (t TeamHoldState) DependsTable() []batch_compute.Table {
	return []batch_compute.Table{
		CurrentOrderHold{},
	}
}

// TableName implements batch_compute.Table.
func (t TeamHoldState) TableName() string {
	return "team_hold_state"
}

// Temporary implements batch_compute.Table.
func (t TeamHoldState) Temporary() bool {
	return false
}
