package performance

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type PickingLog struct{}

// BuildQuery implements [batch_compute.Table].
func (p PickingLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with pl as (
			select 
				it.tx_id,
				max(it.timestamp) filter (where it.status = 'picking') as picking_at,
				max(it.timestamp) filter (where it.status = 'picked') as picked_at
			from public.inv_timestamps it
			where
				date(it.timestamp) > '%s'
			group by it.tx_id
		)

		select 
			it.team_id,
			it.warehouse_id,
			pl.*,
			EXTRACT(EPOCH FROM (pl.picked_at - pl.picking_at))::bigint as picking_duration
		from pl
		left join public.inv_transactions it on it.id = pl.tx_id
		where 
			pl.picked_at is not null
		`,
		graph.Filter.StartDate.Format("2006-01-02"),
	)
}

// TableName implements [batch_compute.Table].
func (p PickingLog) TableName() string {
	return "picking_log"
}

// Temporary implements [batch_compute.Table].
func (p PickingLog) Temporary() bool {
	return true
}
