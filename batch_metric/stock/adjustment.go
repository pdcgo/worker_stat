package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type AdjustmentCreatedLog struct{}

// BuildQuery implements [batch_compute.Table].
func (a AdjustmentCreatedLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select
			it.id as tx_id,
			it.created,
			it.team_id,
			it.warehouse_id,
			case 
				when it.type = 'adj_out' then itc.item_count * -1
				else itc.item_count
			end as item_count,
			
			case 
				when it.type = 'adj_out' then itc.item_amount * -1
				else itc.item_amount
			end as item_amount
			
			
		from public.inv_transactions it
		left join %s itc on itc.tx_id = it.id
		where 
			it.created > '2025-09-09'
			and it.type in ('adj_in', 'adj_out')
			and it.status != 'cancel'
		`,
		graph.DependName(InvItemLog{}),
	)
}

// TableName implements [batch_compute.Table].
func (a AdjustmentCreatedLog) TableName() string {
	return "adjustment_created_log"
}

// Temporary implements [batch_compute.Table].
func (a AdjustmentCreatedLog) Temporary() bool {
	return true
}
