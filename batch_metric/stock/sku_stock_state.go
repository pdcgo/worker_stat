package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type SkuReadyStockState struct{}

// BuildQuery implements [batch_compute.Table].
func (p SkuReadyStockState) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with d as (
			select
				ih.sku_id,
				(ih.count * -1) as count,
				
				(
					ih.price + coalesce(ih.ext_price, 0)
				) as price

			from public.invertory_histories ih
			join public.inv_transactions it on it.id = ih.in_tx_id 
			where 
				ih.tx_id is null
				and ih.in_tx_id is not null
				and it.created > '%s'
		)

		select
			d.sku_id,
			sum(d.count) as item_count,
			sum(d.count * d.price) as item_amount
		from d
		group by d.sku_id
		`,
		graph.Filter.StartDate.Format("2006-01-02"),
	)
}

// TableName implements [batch_compute.Table].
func (p SkuReadyStockState) TableName() string {
	return "sku_ready_stock_state"
}

// Temporary implements [batch_compute.Table].
func (p SkuReadyStockState) Temporary() bool {
	return false
}

type SkuReadyStockOutFilter struct{}

// BuildQuery implements [batch_compute.Table].
func (s SkuReadyStockOutFilter) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with d as (
			select
				ih.sku_id,
				(ih.count * -1) as count,
				
				(
					ih.price + coalesce(ih.ext_price, 0)
				) as price

			from public.invertory_histories ih
			join public.inv_transactions it on it.id = ih.in_tx_id 
			where 
				ih.tx_id is null
				and it.created <= '%s'
		)

		select
			d.sku_id,
			sum(d.count) as item_count,
			sum(d.count * d.price) as item_amount
		from d
		group by d.sku_id
		`,
		graph.Filter.StartDate.Format("2006-01-02"),
	)
}

// TableName implements [batch_compute.Table].
func (s SkuReadyStockOutFilter) TableName() string {
	return "sku_ready_stock_out_filter"
}

// Temporary implements [batch_compute.Table].
func (s SkuReadyStockOutFilter) Temporary() bool {
	return false
}
