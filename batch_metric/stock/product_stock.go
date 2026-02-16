package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type SkuReadyStockState struct{}

// BuildQuery implements [batch_compute.Table].
func (p SkuReadyStockState) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
	with d as (
		select
			ih.sku_id,
			(ih.count * -1) as count,
			
			(
				ih.price + coalesce(ih.ext_price, 0)
			) as price

		from public.invertory_histories ih 
		where 
			ih.tx_id is null
	)

	select
		d.sku_id,
		sum(d.count) as item_count,
		sum(d.count * d.price) as item_amount
	from d
	group by d.sku_id
	`
}

// TableName implements [batch_compute.Table].
func (p SkuReadyStockState) TableName() string {
	return "sku_ready_stock_state"
}

// Temporary implements [batch_compute.Table].
func (p SkuReadyStockState) Temporary() bool {
	return false
}

type SkuReadyStockErr struct{}

// BuildQuery implements [batch_compute.Table].
func (s SkuReadyStockErr) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select
			'1'
		from %s
		union
		select
			'1'
		from %s
		`,
		graph.DependName(s, InboundSpent{}),
		graph.DependName(s, SkuReadyStockState{}),
	)
}

// TableName implements [batch_compute.Table].
func (s SkuReadyStockErr) TableName() string {
	return "sku_ready_stock_err"
}

// Temporary implements [batch_compute.Table].
func (s SkuReadyStockErr) Temporary() bool {
	return false
}
