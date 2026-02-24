package withdrawal

import "github.com/pdcgo/worker_stat/batch_compute"

type OrderDoubleWithdrawal struct{}

// BuildQuery implements [batch_compute.Table].
func (o OrderDoubleWithdrawal) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
	with d as (
		select 
			oa.order_id,
			oa.amount,
			oa.type,
			count(oa.order_id) as count_tx
			
		from order_adjustments oa 
		group by (
			oa.order_id,
			oa.amount,
			oa.type
		)
	)

	select 
		d.*,
		o.team_id,
		o.created_by_id,
		o.order_mp_id,
		o.created_at,
		o.order_ref_id
		
	from d
	left join public.orders o on o.id = d.order_id
	where d.count_tx > 1
	
	`
}

// TableName implements [batch_compute.Table].
func (o OrderDoubleWithdrawal) TableName() string {
	return "order_double_withdrawal"
}

// Temporary implements [batch_compute.Table].
func (o OrderDoubleWithdrawal) Temporary() bool {
	return false
}

// var d batch_compute.Table = OrderDoubleWithdrawal{}
