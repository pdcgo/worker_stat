package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type InvItemLog struct{}

// BuildQuery implements [batch_compute.Table].
func (i InvItemLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
	select
		iti.inv_transaction_id as tx_id,
		sum(iti.count) as item_count,
		sum(iti.total) as item_amount
	from public.inv_tx_items iti 
	group by tx_id
	`
}

// TableName implements [batch_compute.Table].
func (i InvItemLog) TableName() string {
	return "inv_item_log"
}

// Temporary implements [batch_compute.Table].
func (i InvItemLog) Temporary() bool {
	return true
}

type RestockCreatedLog struct{}

// BuildQuery implements [batch_compute.Table].
func (r RestockCreatedLog) BuildQuery(graph *batch_compute.GraphContext) string {
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
			and it.type = 'restock'
			and it.status != 'cancel'
		`,
		graph.DependName(r, InvItemLog{}),
	)
}

// TableName implements batch_compute.Table.
func (r RestockCreatedLog) TableName() string {
	return "restock_created_log"
}

// Temporary implements batch_compute.Table.
func (r RestockCreatedLog) Temporary() bool {
	return true
}

type RestockArrivedLog struct{}

// BuildQuery implements [batch_compute.Table].
func (r RestockArrivedLog) BuildQuery(graph *batch_compute.GraphContext) string {
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
			and it.type = 'restock'
			and it.status != 'cancel'
			and it.arrived is not null
		`,
		graph.DependName(r, InvItemLog{}),
	)
}

// TableName implements batch_compute.Table.
func (r RestockArrivedLog) TableName() string {
	return "restock_arrived_log"
}

// Temporary implements batch_compute.Table.
func (r RestockArrivedLog) Temporary() bool {
	return true
}
