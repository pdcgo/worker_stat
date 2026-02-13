package order

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type TeamCreatedLog struct{}

// BuildQuery implements [batch_compute.Table].
func (t TeamCreatedLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			team_id,
			date(at) as day,
			count(order_id) as tx_count,
			sum(revenue_amount) as revenue_amount
		from %s
		group by (team_id, day)
		`,
		graph.DependName(&OrderCreatedLog{}),
	)
}

// TableName implements batch_compute.Table.
func (t TeamCreatedLog) TableName() string {
	return "team_created_log"
}

// Temporary implements batch_compute.Table.
func (t TeamCreatedLog) Temporary() bool {
	return true
}

type TeamCompletedLog struct{}

// BuildQuery implements [batch_compute.Table].
func (t TeamCompletedLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			team_id,
			date(at) as day,
			count(order_id) as tx_count,
			sum(revenue_amount) as revenue_amount
		from %s
		group by (team_id, day)
		`,
		graph.DependName(OrderCompletedLog{}),
	)
}

// TableName implements batch_compute.Table.
func (t TeamCompletedLog) TableName() string {
	return "team_completed_log"
}

// Temporary implements batch_compute.Table.
func (t TeamCompletedLog) Temporary() bool {
	return true
}

type DailyTeamHold struct{}

// BuildQuery implements [batch_compute.Table].
func (d DailyTeamHold) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with stage as (
			select 
				COALESCE(cr.team_id, com.team_id) team_id,
				COALESCE(cr.day, com.day) as day,
				COALESCE(com.tx_count, 0) as completed_count,
				COALESCE(com.revenue_amount, 0) as completed_revenue_amount,
				COALESCE(cr.tx_count, 0) as created_count,
				COALESCE(cr.revenue_amount, 0) as created_revenue_amount
			from %s cr
			full join %s com on 
				cr.team_id = com.team_id
				and cr.day = com.day
		),
		
		data as (
			select 
				th.*,
				sum(th.created_count - th.completed_count) over (
					partition by th.team_id
					order by th.day asc) as hold_count,
				sum(th.created_revenue_amount - th.completed_revenue_amount) over (
					partition by th.team_id
					order by th.day asc) as hold_amount
			from stage th
		)
		
		select 
			*,
			now() as sync_at
		from data
		`,
		graph.DependName(TeamCreatedLog{}),
		graph.DependName(TeamCompletedLog{}),
	)
}

// TableName implements batch_compute.Table.
func (d DailyTeamHold) TableName() string {
	return "daily_team_holds"
}

// Temporary implements batch_compute.Table.
func (d DailyTeamHold) Temporary() bool {
	return false
}
