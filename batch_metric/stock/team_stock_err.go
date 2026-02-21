package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type TeamReadyStockState struct{}

// BuildQuery implements [batch_compute.Table].
func (t TeamReadyStockState) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			s.team_id,
			sum(ss.item_count) as count,
			sum(ss.item_amount) as amount
		from %s ss
		left join public.skus s on s.id = ss.sku_id
		group by s.team_id
		
		`,
		graph.DependName(t, SkuReadyStockState{}),
	)
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
			(h.ready_count - s.count) as count_err
			
			
			
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
