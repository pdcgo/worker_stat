package performance

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type DailyWarehouseCompleted struct{}

// BuildQuery implements [batch_compute.Table].
func (d DailyWarehouseCompleted) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select
			cl.warehouse_id,
			date(cl.picking_at) as day,
			
			count(cl.tx_id) as tx_count,
			avg(cl.completed_duration) as duration_avg,
			percentile_cont(0.9) WITHIN GROUP (ORDER BY cl.completed_duration) as duration_90,
			percentile_cont(0.75) WITHIN GROUP (ORDER BY cl.completed_duration) as duration_75,
			percentile_cont(0.5) WITHIN GROUP (ORDER BY cl.completed_duration) as duration_50
		from %s cl 
		group by (
			cl.warehouse_id,
			day
		)
		`,
		graph.DependName(d, CompletedLog{}),
	)
}

// TableName implements [batch_compute.Table].
func (d DailyWarehouseCompleted) TableName() string {
	return "daily_warehouse_completed"
}

// Temporary implements [batch_compute.Table].
func (d DailyWarehouseCompleted) Temporary() bool {
	return false
}

type DailyWarehousePicking struct{}

// BuildQuery implements [batch_compute.Table].
func (d DailyWarehousePicking) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select
			pl.warehouse_id,
			date(pl.picking_at) as day,
			
			count(pl.tx_id) as tx_count,
			avg(pl.picking_duration) as duration_avg,
			percentile_cont(0.9) WITHIN GROUP (ORDER BY pl.picking_duration) as duration_90,
			percentile_cont(0.75) WITHIN GROUP (ORDER BY pl.picking_duration) as duration_75,
			percentile_cont(0.5) WITHIN GROUP (ORDER BY pl.picking_duration) as duration_50
		from %s pl 
		group by (
			pl.warehouse_id,
			day
		)
		`,
		graph.DependName(d, PickingLog{}),
	)
}

// TableName implements [batch_compute.Table].
func (d DailyWarehousePicking) TableName() string {
	return "daily_warehouse_picking"
}

// Temporary implements [batch_compute.Table].
func (d DailyWarehousePicking) Temporary() bool {
	return false
}

type DailyUserPicking struct{}

var _ batch_compute.Table = (*DailyUserPicking)(nil)

// BuildQuery implements [batch_compute.Table].
func (d DailyUserPicking) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with d as (
			select 
				pl.*,
				it.user_id
			from %s pl 
			left join public.inv_timestamps it on it.tx_id = pl.tx_id
			where 
				it.status = 'picking'
		)

		select
			pl.user_id,
			date(pl.picking_at) as day,
			
			count(pl.tx_id) as tx_count,
			avg(pl.picking_duration) as duration_avg,
			percentile_cont(0.9) WITHIN GROUP (ORDER BY pl.picking_duration) as duration_90,
			percentile_cont(0.75) WITHIN GROUP (ORDER BY pl.picking_duration) as duration_75,
			percentile_cont(0.5) WITHIN GROUP (ORDER BY pl.picking_duration) as duration_50
		from d pl 
		group by (
			pl.user_id,
			day
		)
		`,
		graph.DependName(d, PickingLog{}),
	)
}

// TableName implements [batch_compute.Table].
func (d DailyUserPicking) TableName() string {
	return "daily_user_picking"
}

// Temporary implements [batch_compute.Table].
func (d DailyUserPicking) Temporary() bool {
	return false
}
