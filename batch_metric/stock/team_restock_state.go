package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type RestockState struct{}

// BuildQuery implements [batch_compute.Table].
func (t RestockState) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select
			it.id as tx_id,
			it.created,
			it.team_id,
			it.warehouse_id,
			it.shipping_id,
			rc.payment_type,
			itc.item_count,
			itc.item_amount,
			it.total as total_amount
		from public.inv_transactions it
		join public.restock_costs rc on rc.inv_transaction_id = it.id
		left join %s itc on itc.tx_id = it.id
		where 
			it.created > '2025-09-09'
			and it.type = 'restock'
			and it.status != 'cancel'
			and it.arrived is null
		`,
		graph.DependName(InvItemLog{}),
	)
}

// TableName implements [batch_compute.Table].
func (t RestockState) TableName() string {
	return "restock_state"
}

// Temporary implements [batch_compute.Table].
func (t RestockState) Temporary() bool {
	return true
}

type TeamRestockState struct{}

// BuildQuery implements [batch_compute.Table].
func (t TeamRestockState) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			rs.team_id,
			count(rs.tx_id) as tx_count,
			sum(rs.item_count) as item_count,
			sum(rs.total_amount) as total_amount
		from %s rs 
		group by rs.team_id
		`,
		graph.DependName(RestockState{}),
	)
}

// TableName implements [batch_compute.Table].
func (t TeamRestockState) TableName() string {
	return "team_restock_state"
}

// Temporary implements [batch_compute.Table].
func (t TeamRestockState) Temporary() bool {
	return false
}
