package stock

import (
	"github.com/pdcgo/worker_stat/batch_compute"
)

type BrokenCreatedLog struct{}

// BuildQuery implements [batch_compute.Table].
func (r BrokenCreatedLog) BuildQuery(graph *batch_compute.GraphContext) string {

	return `
	select 
		p.tx_id,
		it2.created,
		it2.team_id,
		it2.warehouse_id,
		iti.count as item_count,
		iti.total as amount,
		
		iti.*
		
	from public.inv_item_problems p
	left join public.inv_tx_items iti on iti.id = p.tx_item_id
	left join public.inv_transactions it on it.id = iti.inv_transaction_id
	left join public.inv_transactions it2 on it2.id = p.tx_id

	where
		it.created > '2025-09-09'
		and it2.status != 'cancel'
	`
}

// TableName implements batch_compute.Table.
func (r BrokenCreatedLog) TableName() string {
	return "broken_created_log"
}

// Temporary implements batch_compute.Table.
func (r BrokenCreatedLog) Temporary() bool {
	return true
}
