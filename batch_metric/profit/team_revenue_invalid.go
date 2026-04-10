package profit

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type TeamOrderRevenueInvalid struct{}

// BuildQuery implements [batch_compute.Table].
func (t TeamOrderRevenueInvalid) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with d as (
			select 
				coalesce(dw.day, tor.day) as day,
				coalesce(dw.team_id, tor.team_id) as team_id,
				coalesce(dw.amount, 0) as csv_amount,
				tor.total_revenue_amount
			from %s dw
			full join %s tor on tor.team_id = dw.team_id and tor.day = dw.day
		),

		man as (
			select 
			date(al.fund_at) as day,
			al.team_id,
			sum(al.import_amount) as import_revenue_amount,
			sum(al.manual_amount) as manual_revenue_amount
			from %s al
			group by (day, team_id)
		)

		select
			d.day,
			d.team_id,
			d.total_revenue_amount,
			d.csv_amount,
			man.import_revenue_amount,
			man.manual_revenue_amount,
			(
				d.csv_amount + man.import_revenue_amount
			) as invalid_amount
		from d
		left join man on man.day = d.day and man.team_id = d.team_id
		order by d.day desc
		`,
		graph.DependName(t, TeamDailyWithdrawal{}),
		graph.DependName(t, TeamOrderRevenue{}),
		graph.DependName(t, AdjustmentLog{}),
	)
}

// TableName implements [batch_compute.Table].
func (t TeamOrderRevenueInvalid) TableName() string {
	return "team_order_revenue_invalid"
}

// Temporary implements [batch_compute.Table].
func (t TeamOrderRevenueInvalid) Temporary() bool {
	return false
}
