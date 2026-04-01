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
			
			case d.seed
				when 0 then 1
			else 0 
			end as additional_order_count,
			
			case d.seed
				when 0 then d.amount
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

type TeamOrderRevenue struct{}

func (t TeamOrderRevenue) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			date(al.fund_at) as day,
			al.team_id,
			sum(al.amount) as total_revenue_amount,
			
			sum(al.real_revenue_amount) as real_revenue_amount,
			sum(al.estimated_revenue_amount) as estimated_revenue_amount,
			
			sum(al.adj_real_revenue_amount) as adj_real_revenue_amount,
			
			
			sum(al.additional_order_count) as additional_order_count,
			sum(al.additional_revenue_amount) as additional_revenue_amount
		from %s al
		group by (
			day,
			al.team_id
		)
		`,
		graph.DependName(t, AdjustmentLog{}),
	)
}

func (t TeamOrderRevenue) TableName() string {
	return "team_order_revenue"
}

func (t TeamOrderRevenue) Temporary() bool {
	return false
}

type TeamProfit struct{}

// BuildQuery implements [batch_compute.Table].
func (t TeamProfit) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select * from %s
		`,
		graph.DependName(t, TeamOrderRevenue{}),
	)
}

// TableName implements [batch_compute.Table].
func (t TeamProfit) TableName() string {
	return "team_profit"
}

// Temporary implements [batch_compute.Table].
func (t TeamProfit) Temporary() bool {
	return false
}
