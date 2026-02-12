package order

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type dailyLastTeamHold struct{}

// CreateQuery implements batch_compute.Table.
func (d dailyLastTeamHold) CreateQuery(schema batch_compute.Schema) string {
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
		schema.GetTableName(DailyTeamHold{}),
		schema.GetTableName(DailyTeamHold{}),
	)
}

// DependsTable implements batch_compute.Table.
func (d dailyLastTeamHold) DependsTable() []batch_compute.Table {
	return []batch_compute.Table{
		DailyTeamHold{},
	}
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

// CreateQuery implements batch_compute.Table.
func (t TeamHoldErr) CreateQuery(schema batch_compute.Schema) string {
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
		schema.GetTableName(dailyLastTeamHold{}),
		schema.GetTableName(TeamHoldState{}),
	)
}

// DependsTable implements batch_compute.Table.
func (t TeamHoldErr) DependsTable() []batch_compute.Table {
	return []batch_compute.Table{
		dailyLastTeamHold{},
		TeamHoldState{},
	}
}

// TableName implements batch_compute.Table.
func (t TeamHoldErr) TableName() string {
	return "team_hold_err"
}

// Temporary implements batch_compute.Table.
func (t TeamHoldErr) Temporary() bool {
	return false
}
