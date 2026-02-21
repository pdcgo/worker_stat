package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type SkuReadyStockErr struct{}

// BuildQuery implements [batch_compute.Table].
func (s SkuReadyStockErr) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with d as (
			select 
				ins.sku_id,
				sum(count_err) as item_count,
				sum(amount_err) as item_amount
			from %s ins
			where 
				ins.count_err > 0
				or ins.amount_err > 0
			group by
				ins.sku_id
		),
		
		final as (
			select 
				sr.sku_id,
				sr.item_count as state_count,
				sr.item_amount as state_amount,
				d.item_count,
				d.item_amount,
				(sr.item_count - coalesce(d.item_count, 0)) as count_err,
				(sr.item_amount - coalesce(d.item_amount, 0)) as amount_err
			from %s sr
			left join d on d.sku_id = sr.sku_id
		)
		
		select 
			*
		from final
		where 
			count_err != 0
			or amount_err != 0
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
