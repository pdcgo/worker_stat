package performance

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type CompletedLog struct{}

// BuildQuery implements [batch_compute.Table].
func (c CompletedLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with cl as (
			select 
				it.tx_id,
				max(it.timestamp) filter (where it.status = 'completed') as completed_at,
				max(it.timestamp) filter (where it.status = 'picking') as picking_at
			from public.inv_timestamps it
			where
				date(it.timestamp) > '%s'
			group by it.tx_id
		)

		select 
			it.team_id,
			it.warehouse_id,
			cl.*,
			EXTRACT(EPOCH FROM (cl.completed_at - cl.picking_at))::bigint as completed_duration
		from cl
		left join public.inv_transactions it on it.id = cl.tx_id
		where 
			cl.completed_at is not null
			and cl.picking_at is not null
		`,
		graph.Filter.StartDate.Format("2006-01-02"),
	)
}

// TableName implements [batch_compute.Table].
func (c CompletedLog) TableName() string {
	return "completed_log"
}

// Temporary implements [batch_compute.Table].
func (c CompletedLog) Temporary() bool {
	return true
}
