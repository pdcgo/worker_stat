package order

import "github.com/pdcgo/worker_stat/batch_compute"

type OrderItemLog struct{}

// CreateQuery implements batch_compute.Table.
func (o *OrderItemLog) CreateQuery(schema batch_compute.Schema) string {

	return `
	select 
		oi.order_id,	
		oi.variation_id,
		oi.count,
		oi.price,
		oi.total,
		oi.owned,
		o.team_id,
		o.order_mp_id as shop_id,
		o.created_by_id as user_id,
		it.warehouse_id,
		
		o.order_mp_total as revenue_amount,
		o.created_at
		
	from order_items oi
	join orders o on o.id = oi.order_id
	join public.inv_transactions it on it.id = o.invertory_tx_id
	where
		o.is_partial != true
		and o.is_order_fake != true
		and o.status != 'cancel'
	
	`
}

// DependsTable implements batch_compute.Table.
func (o *OrderItemLog) DependsTable() []batch_compute.Table {
	return []batch_compute.Table{}
}

// TableName implements batch_compute.Table.
func (o *OrderItemLog) TableName() string {
	return "order_item_log"
}

// Temporary implements batch_compute.Table.
func (o *OrderItemLog) Temporary() bool {
	return true
}
