package order

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type dailyLastTeamHold struct{}

// BuildQuery implements [batch_compute.Table].
func (d dailyLastTeamHold) BuildQuery(graph *batch_compute.GraphContext) string {

	tableName := graph.DependName(DailyTeamHold{})

	return fmt.Sprintf(
		`
		select 
			dsh.*
		from %s dsh
		join (
			select
				max(day) as day,
				team_id
			from %s dsh
			group by team_id
		) l on l.day = dsh.day and l.team_id = dsh.team_id
		`,
		tableName,
		tableName,
	)
}

// TableName implements batch_compute.Table.
func (d dailyLastTeamHold) TableName() string {
	return "last_daily_team_hold"
}

// Temporary implements batch_compute.Table.
func (d dailyLastTeamHold) Temporary() bool {
	return true
}

type TeamHoldErr struct{}

// BuildQuery implements [batch_compute.Table].
func (t TeamHoldErr) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with data as (
			select
				l.team_id,
				(l.hold_count - c.tx_count) as hold_count_err,
				(l.hold_amount - c.revenue_amount) as hold_amount_err
				
			from %s l
			left join %s c
				on l.team_id = c.team_id
		)
		select 
			*,
			now() as sync_at
		from data
		where 
			hold_amount_err != 0 
			or hold_count_err != 0
		`,
		graph.DependName(dailyLastTeamHold{}),
		graph.DependName(TeamHoldState{}),
	)
}

// TableName implements batch_compute.Table.
func (t TeamHoldErr) TableName() string {
	return "team_hold_err"
}

// Temporary implements batch_compute.Table.
func (t TeamHoldErr) Temporary() bool {
	return false
}
