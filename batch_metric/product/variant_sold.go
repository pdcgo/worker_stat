package product

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/pdcgo/worker_stat/batch_metric/order"
)

type VariantSold struct{}

// BuildQuery implements [batch_compute.Table].
func (v VariantSold) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(`
	with vardata as (
		select 
			od.variation_id,
			od.warehouse_id,
			date(od.created_at) as day,
			count(od.order_id) as tx_count,
			sum(od.count) filter (where od.owned != true) as cross_item_count,
			sum(od.total) filter (where od.owned != true) as cross_cost_amount,
			sum(od.count) filter (where od.owned = true) as owned_item_count,
			sum(od.total) filter (where od.owned = true) as owned_cost_amount,
			sum(od.count) as item_count,
			sum(od.total) as cost_amount
			
		from %s od
		group by (
			od.variation_id,
			od.warehouse_id,
			day
		)
	)
	select 
		vs.*,
		avg(item_count) OVER (
			ORDER BY day asc
			ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
		) as avg_item_count,
		avg(cost_amount) OVER (
			ORDER BY day asc
			ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
		) as avg_cost_amount
	from vardata vs
	`,
		graph.DependName(v, &order.OrderItemLog{}),
	)
}

// TableName implements batch_compute.Table.
func (v VariantSold) TableName() string {
	return "variant_sold"
}

// Temporary implements batch_compute.Table.
func (v VariantSold) Temporary() bool {
	return false
}
