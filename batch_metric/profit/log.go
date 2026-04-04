package profit

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type AdjustmentLog struct{}

func (a AdjustmentLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with al as (
			select 
				oa.id,
				o.team_id as team_id,
				o.created_by_id as user_id,
				o.order_mp_id as shop_id,
				oa.order_id,
				oa.fund_at,
				oa.type as adj_type,
				oa.amount
				
			from public.order_adjustments oa 
			left join public.orders o on o.id = oa.order_id
			where 
				o.created_at > '%s'
		),

		d as (
			select
				al.*,
				count(1) over (partition by al.order_id order by al.id asc) as seed 
			from al
		)

		select
			d.*,
			case d.seed
				when 1 then o.order_mp_total
			else 0 
			end as estimated_revenue_amount,

			case d.seed
				when 1 then o.total
			else 0 
			end as order_cost_amount,
			
			case d.seed
				when 1 then 1
			else 0 
			end as order_count,

			case d.seed
				when 1 then d.amount
			else 0 
			end as real_revenue_amount,
			
			case d.seed
				when 1 then d.amount - o.order_mp_total
			else 0 
			end as adj_real_revenue_amount,
			
			case
				when d.seed > 1 then 1
			else 0 
			end as additional_order_count,
			
			case
				when d.seed > 1 then d.amount
			else 0 
			end as additional_revenue_amount

			
		from d
		left join public.orders o on o.id = d.order_id
		`,
		graph.Filter.StartDate.Format("2006-01-02"),
	)
}

func (a AdjustmentLog) TableName() string {
	return "adjustment_log"
}

func (a AdjustmentLog) Temporary() bool {
	return false
}
