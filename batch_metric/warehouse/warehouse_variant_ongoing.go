package warehouse

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/pdcgo/worker_stat/batch_metric/warehouse/warehouse_transaction"
)

type WarehouseVariantOngoingStock struct{}

// TableName implements [batch_compute.Table].
func (t WarehouseVariantOngoingStock) TableName() string {
	return "warehouse_variant_ongoing_stock"
}

// Temporary implements [batch_compute.Table].
func (t WarehouseVariantOngoingStock) Temporary() bool {
	return false
}

// BuildQuery implements [batch_compute.Table].
func (t WarehouseVariantOngoingStock) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			il.variant_id as variation_id,
			il.warehouse_id,
			sum(item_count) as item_count,
			sum(item_amount) as item_amount
			
		from %s il
		where
			il.type in ('return', 'restock')
			and il.arrived_at is null
		group by (
			variant_id,
			warehouse_id
		)
		
		`,
		graph.DependName(t, &warehouse_transaction.TxItemLog{}),
	)
}
