package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type ReturnCreatedLog struct{}

// BuildQuery implements [batch_compute.Table].
func (r ReturnCreatedLog) BuildQuery(graph *batch_compute.GraphContext) string {
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
			it.total as amount
			
		from public.inv_transactions it
		join public.restock_costs rc on rc.inv_transaction_id = it.id
		left join %s itc on itc.tx_id = it.id
		where 
			it.created > '2025-09-09'
			and it.type = 'return'
			and it.status != 'cancel'
		`,
		graph.DependName(InvItemLog{}),
	)
}

// TableName implements batch_compute.Table.
func (r ReturnCreatedLog) TableName() string {
	return "return_created_log"
}

// Temporary implements batch_compute.Table.
func (r ReturnCreatedLog) Temporary() bool {
	return true
}

type ReturnArrivedLog struct{}

// BuildQuery implements [batch_compute.Table].
func (r ReturnArrivedLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select
			it.id as tx_id,
			it.arrived,
			it.team_id,
			it.warehouse_id,
			it.shipping_id,
			rc.payment_type,
			itc.item_count,
			it.total as amount
			
		from public.inv_transactions it
		join public.restock_costs rc on rc.inv_transaction_id = it.id
		left join %s itc on itc.tx_id = it.id
		where 
			it.created > '2025-09-09'
			and it.type = 'return'
			and it.status != 'cancel'
		`,
		graph.DependName(InvItemLog{}),
	)
}

// TableName implements batch_compute.Table.
func (r ReturnArrivedLog) TableName() string {
	return "return_arrived_log"
}

// Temporary implements batch_compute.Table.
func (r ReturnArrivedLog) Temporary() bool {
	return true
}
