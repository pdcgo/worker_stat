package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type DailyTeamReturnArrived struct{}

// BuildQuery implements [batch_compute.Table].
func (d DailyTeamReturnArrived) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			date(c.arrived) as day,
			c.team_id as team_id,
			count(c.tx_id) as tx_count,
			sum(c.item_count) as item_count,
			sum(c.amount) filter (where c.payment_type = 'bank_account') as bank_amount,
			sum(c.amount) filter (where c.payment_type = 'shopee_pay') as shopeepay_amount,
			sum(c.amount) as total_amount
			
		from %s c
		group by (
			day,
			team_id
		)
		`,
		graph.DependName(d, ReturnArrivedLog{}),
	)
}

// TableName implements batch_compute.Table.
func (d DailyTeamReturnArrived) TableName() string {
	return "daily_team_return_arrived"
}

// Temporary implements batch_compute.Table.
func (d DailyTeamReturnArrived) Temporary() bool {
	return true
}

type DailyTeamReturnCreated struct{}

// BuildQuery implements [batch_compute.Table].
func (d DailyTeamReturnCreated) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			date(c.created) as day,
			c.team_id as team_id,
			count(c.tx_id) as tx_count,
			sum(c.item_count) as item_count,
			sum(c.amount) filter (where c.payment_type = 'bank_account') as bank_amount,
			sum(c.amount) filter (where c.payment_type = 'shopee_pay') as shopeepay_amount,
			sum(c.amount) as total_amount
			
		from %s c
		group by (
			day,
			team_id
		)
		`,
		graph.DependName(d, ReturnCreatedLog{}),
	)
}

// TableName implements [batch_compute.Table].
func (d DailyTeamReturnCreated) TableName() string {
	return "daily_team_return_created"
}

// Temporary implements [batch_compute.Table].
func (d DailyTeamReturnCreated) Temporary() bool {
	return true
}

type DailyTeamReturn struct{}

// BuildQuery implements [batch_compute.Table].
func (d DailyTeamReturn) BuildQuery(graph *batch_compute.GraphContext) string {

	return fmt.Sprintf(
		`
		with stage as (
			select 
				coalesce(cr.day, ar.day) as day,
				coalesce(cr.team_id, ar.team_id) as team_id,

				coalesce(cr.tx_count, 0) as created_tx_count,
				coalesce(ar.tx_count, 0) as arrived_tx_count,

				coalesce(cr.item_count, 0) as created_item_count,
				coalesce(cr.total_amount, 0) as created_total_amount,

				coalesce(ar.item_count, 0) as arrived_item_count,
				coalesce(ar.total_amount, 0) as arrived_total_amount
				
				
				
				
			from %s cr
			full join %s ar on
				cr.day = ar.day	
				and cr.team_id = ar.team_id 
		),	

		data as (
			select 
				s.*,
				sum(s.created_tx_count - s.arrived_tx_count) over (
					partition by s.team_id
					order by s.day asc
				) as hold_tx_count,
				
				sum(s.created_item_count - s.arrived_item_count) over (
					partition by s.team_id
					order by s.day asc
				) as hold_item_count,

				sum(s.created_total_amount - s.arrived_total_amount) over (
					partition by s.team_id
					order by s.day asc
				) as hold_amount

			from stage s
		)
		
		select 
			*,
			now() as sync_at
		from data
		`,
		graph.DependName(d, DailyTeamReturnCreated{}),
		graph.DependName(d, DailyTeamReturnArrived{}),
	)
}

// TableName implements batch_compute.Table.
func (d DailyTeamReturn) TableName() string {
	return "daily_team_return"
}

// Temporary implements batch_compute.Table.
func (d DailyTeamReturn) Temporary() bool {
	return false
}
