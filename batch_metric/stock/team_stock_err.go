package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type TeamReadyStockState struct{}

// BuildQuery implements [batch_compute.Table].
func (t TeamReadyStockState) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
		
	with d as (
		select
			ih.team_id,
			(ih.count * -1) as count,
			
			(
				ih.price + coalesce(ih.ext_price, 0)
			) as price

		from public.invertory_histories ih 
		where 
			ih.tx_id is null
	)

	select
		d.team_id,
		sum(d.count) as count,
		sum(d.count * d.price) as amount
	from d
	group by d.team_id
	
	`
}

// TableName implements [batch_compute.Table].
func (t TeamReadyStockState) TableName() string {
	return "team_ready_stock_state"
}

// Temporary implements [batch_compute.Table].
func (t TeamReadyStockState) Temporary() bool {
	return true
}

type TeamStockErr struct{}

// BuildQuery implements [batch_compute.Table].
func (t TeamStockErr) BuildQuery(graph *batch_compute.GraphContext) string {
	dsName := graph.DependName(t, TeamDailyStock{})
	return fmt.Sprintf(
		`
		with l as (
			select 
				max(ds.day) as day,
				ds.team_id
			from %s ds
			group by ds.team_id
		),

		h as (
			select 
				d.*
			from l
			join %s d on
				d.team_id = l.team_id
				and d.day = l.day
		)

		select 
			h.team_id,
			(s.count - h.ready_count) as count_err
			
			
			
		from h
		left join %s s on
			s.team_id = h.team_id
		`,
		dsName,
		dsName,
		graph.DependName(t, TeamReadyStockState{}),
	)
}

// TableName implements [batch_compute.Table].
func (t TeamStockErr) TableName() string {
	return "team_stock_err"
}

// Temporary implements [batch_compute.Table].
func (t TeamStockErr) Temporary() bool {
	return false
}
