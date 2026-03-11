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

type MetricVariantSold struct{}

// BuildQuery implements [batch_compute.Table].
func (l MetricVariantSold) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(`
	select 
		vs.*
	from %s vs
	where vs.day >= current_date - interval '7 days'
	`,
		graph.DependName(l, VariantSold{}),
	)
}

// TableName implements [batch_compute.Table].
func (l MetricVariantSold) TableName() string {
	return "metric_variant_sold"
}

// Temporary implements [batch_compute.Table].
func (l MetricVariantSold) Temporary() bool {
	return false
}

// 94	Palem Warehouse
// 96	Cemara Warehouse
// 92	LGIS Warehouse
// 39	Pdc Srengat
// 67	Febri Warehouse
// 38	PDC Warehouse

// "name": "Gift Cermin Bulat Mini ",
// "ref_id": "00-0001-V-X-2281-237B",
// "daily_sold_santo": "1327.0",
// "daily_sold_febri": "849.0",
// "daily_sold_palem": "1677.4285714285713",
// "daily_sold_cemara": "1508.1428571428571",
// "stock_ready_santo": "7120",
// "stock_ready_amount_santo": "4011272.3809523811",
// "stock_ready_febri": "4652",
// "stock_ready_amount_febri": "2632385.888888889",
// "stock_ready_palem": "9196",
// "stock_ready_amount_palem": "5261492.111111111",
// "stock_ready_cemara": "9493",
// "stock_ready_amount_cemara": "5429552.861111111",
// "stock_ongoing_santo": "0",
// "stock_ongoing_amount_santo": "0.0",
// "stock_ongoing_febri": "0",
// "stock_ongoing_amount_febri": "0.0",
// "stock_ongoing_palem": "0",
// "stock_ongoing_amount_palem": "0.0",
// "stock_ongoing_cemara": "0",
// "stock_ongoing_amount_cemara": "0.0",
// "markup_in_percent": "20.0"
