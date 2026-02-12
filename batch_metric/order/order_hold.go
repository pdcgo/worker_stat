package order

import "github.com/pdcgo/worker_stat/batch_compute"

type CurrentOrderHold struct{}

// CreateQuery implements batch_compute.Table.
func (c CurrentOrderHold) CreateQuery(schema batch_compute.Schema) string {
	return `
	select 
		date(o.created_at) as day,
		o.team_id,
		o.order_mp_id as shop_id,
		o.created_by_id as user_id,
		count(id) as tx_count,
		sum(o.order_mp_total) as revenue_amount
		
	from orders o 
	where 
		o.status not in ('completed', 'return_problem', 'return_completed', 'return', 'problem', 'cancel')
		and o.created_at > '2025-09-09'
		and o.is_partial != true
		and o.is_order_fake != true
	group by (
		day,
		team_id,
		shop_id,
		user_id
	)
	`
}

// DependsTable implements batch_compute.Table.
func (c CurrentOrderHold) DependsTable() []batch_compute.Table {
	return []batch_compute.Table{}
}

// TableName implements batch_compute.Table.
func (c CurrentOrderHold) TableName() string {
	return "current_order_hold"
}

// Temporary implements batch_compute.Table.
func (c CurrentOrderHold) Temporary() bool {
	return false
}
