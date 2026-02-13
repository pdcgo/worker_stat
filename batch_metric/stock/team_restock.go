package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type DailyTeamRestockArrived struct{}

// CreateQuery implements batch_compute.Table.
func (d DailyTeamRestockArrived) CreateQuery(schema batch_compute.Schema) string {
	return fmt.Sprintf(
		`
		select
			date(sa.arrived) as day,
			sa.team_id,
			sa.warehouse_id,
			sum(sa.amount) as amount
		from %s sa
		group by (
			day,
			team_id,
			warehouse_id
		)
		`,
		schema.GetTableName(RestockArrivedLog{}),
	)
}

// DependsTable implements batch_compute.Table.
func (d DailyTeamRestockArrived) DependsTable() []batch_compute.Table {
	return []batch_compute.Table{
		RestockArrivedLog{},
	}
}

// TableName implements batch_compute.Table.
func (d DailyTeamRestockArrived) TableName() string {
	return "daily_team_restock_arrived"
}

// Temporary implements batch_compute.Table.
func (d DailyTeamRestockArrived) Temporary() bool {
	return true
}

type DailyTeamRestock struct{}

// BuildQuery implements [batch_compute.Table].
func (d DailyTeamRestock) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
		select '1'
	`
}

// TableName implements batch_compute.Table.
func (d DailyTeamRestock) TableName() string {
	return "daily_team_restock"
}

// Temporary implements batch_compute.Table.
func (d DailyTeamRestock) Temporary() bool {
	return false
}
