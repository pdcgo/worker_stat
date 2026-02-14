package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type TeamDailyOutbound struct{}

// BuildQuery implements [batch_compute.Table].
func (t TeamDailyOutbound) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select
			coalesce(os.day, bc.day) as day,
			coalesce(os.team_id, bc.team_id) as team_id,
			
			(
				coalesce(os.total_spent_count, 0) + coalesce(bc.item_count, 0)
			) as outbound_count
			
			
		from %s os
		full join %s bc on 
			bc.team_id = os.team_id
			and bc.day = os.day 
		`,
		graph.DependName(DailyTeamOrderSpent{}),
		graph.DependName(DailyTeamBrokenCreated{}),
	)
}

// TableName implements [batch_compute.Table].
func (t TeamDailyOutbound) TableName() string {
	return "team_daily_outbound"
}

// Temporary implements [batch_compute.Table].
func (t TeamDailyOutbound) Temporary() bool {
	return true
}

type TeamDailyInbound struct{}

// BuildQuery implements [batch_compute.Table].
func (t TeamDailyInbound) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			coalesce(re.day, ret.day) as day,
			coalesce(re.team_id, ret.team_id) as team_id,

			(
				coalesce(re.item_count, 0) 
				+ coalesce(ret.item_count, 0)
			) as inbound_count
			
			
			
			
		from %s re
		full join %s ret on
			ret.team_id = re.team_id
			and ret.day = re.day 
		`,
		graph.DependName(DailyTeamRestockArrived{}),
		graph.DependName(DailyTeamReturnArrived{}),
	)
}

// TableName implements [batch_compute.Table].
func (t TeamDailyInbound) TableName() string {
	return "team_daily_inbound"
}

// Temporary implements [batch_compute.Table].
func (t TeamDailyInbound) Temporary() bool {
	return true
}

type TeamDailyStock struct{}

// BuildQuery implements [batch_compute.Table].
func (t TeamDailyStock) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with d as (
			select 
				coalesce(ib.day, ob.day) as day,
				coalesce(ib.team_id, ob.team_id) as team_id,
				
				ib.inbound_count,
				ob.outbound_count
					
			from %s ib
			full join %s ob on
				ib.team_id = ob.team_id
				and ib.day = ob.day 
		),

		d2 as (
			select 
				coalesce(d.day, ad.day) as day,
				coalesce(d.team_id, ad.team_id) as team_id,
			
				d.inbound_count,
				d.outbound_count,
				ad.item_count as adjustment_count

			from d
			full join %s ad on
				ad.team_id = d.team_id
				and ad.day = d.day
		)


		select 
			*,
			sum(
				coalesce(d.inbound_count, 0)
				- coalesce(d.outbound_count, 0)
				+ coalesce(d.adjustment_count, 0)
			) over (
				partition by d.team_id
				order by d.day asc
			) as ready_count
			
		from d2 d
		`,
		graph.DependName(TeamDailyInbound{}),
		graph.DependName(TeamDailyOutbound{}),
		graph.DependName(DailyTeamAdjustmentCreated{}),
	)
}

// TableName implements [batch_compute.Table].
func (t TeamDailyStock) TableName() string {
	return "team_daily_stock"
}

// Temporary implements [batch_compute.Table].
func (t TeamDailyStock) Temporary() bool {
	return false
}
