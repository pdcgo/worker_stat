package order

import "github.com/pdcgo/worker_stat/batch_compute"

type OrderCompletedLog struct{}

// BuildQuery implements [batch_compute.Table].
func (o OrderCompletedLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
	with completed as (
		select 
			ot.order_id as order_id,
			
			min(ot.timestamp) as at,
			count(ot.id) as change_count,
			min(o.order_mp_total) as revenue_amount
		from order_timestamps ot
		join orders o on o.id = ot.order_id 
		where 
			ot.order_status in ('completed', 'return_problem', 'return', 'return_completed', 'cancel')
			and o.is_partial != true
			and o.is_order_fake != true
			and o.created_at > '2025-09-09'
		group by order_id
	)

	select 
		o.team_id,
		o.order_mp_id as shop_id,
		o.created_by_id as user_id,
		com.*
	from completed com
	join orders o on o.id = com.order_id
	`
}

// TableName implements batch_compute.Table.
func (o OrderCompletedLog) TableName() string {
	return "order_completed_log"
}

// Temporary implements batch_compute.Table.
func (o OrderCompletedLog) Temporary() bool {
	return true
}
