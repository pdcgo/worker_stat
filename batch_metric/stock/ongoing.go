package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type VariantOngoingStock struct{}

// TableName implements [batch_compute.Table].
func (o VariantOngoingStock) TableName() string {
	return "ongoing_stock"
}

// Temporary implements [batch_compute.Table].
func (o VariantOngoingStock) Temporary() bool {
	return true
}

// BuildQuery implements [batch_compute.Table].
func (o VariantOngoingStock) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(`
		select 
			it.id as tx_id,
			it.created,
			it.team_id,
			it.warehouse_id,
			it.shipping_id,
			itc.item_count,
			itc.item_amount,
			it.total as total_amount
		from public.inv_transactions it
		left join %s itc on itc.tx_id = it.id
		where 
			it.created > '2025-09-09'
			and it.type in ('restock', 'return')
			and it.status != 'cancel'
			and it.arrived is null
		`,
		graph.DependName(o, InvItemLog{}),
	)
}
