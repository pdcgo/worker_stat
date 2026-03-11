package warehouse_transaction

import "github.com/pdcgo/worker_stat/batch_compute"

type TxItemLog struct{}

// TableName implements [batch_compute.Table].
func (t TxItemLog) TableName() string {
	return "tx_item_log"
}

// Temporary implements [batch_compute.Table].
func (t TxItemLog) Temporary() bool {
	return true
}

// BuildQuery implements [batch_compute.Table].
func (t TxItemLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
	select 
		s.variant_id,
		it.team_id,
		it.warehouse_id,
		it.type,
		it.created as created_at,
		it.arrived as arrived_at,
		iti.count as item_count,
		iti.total as item_amount
		
	from public.inv_tx_items iti 
	left join public.skus s on s.id = iti.sku_id
	left join public.inv_transactions it on it.id = iti.inv_transaction_id
	where 
		it.created > '2025-09-09'
	
	
	`
}
