package stock_overflow

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/pdcgo/worker_stat/batch_metric/stock"
)

type OutboundSkuLog struct{}

// BuildQuery implements [batch_compute.Table].
func (o OutboundSkuLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			it.created,
			sl.in_tx_id,
			sl.sku_id,
			ih.tx_id,
			ih.count as out_item_count,
			(ih.price + coalesce(ih.ext_price, 0)) as out_item_price
		from %s sl
		left join public.invertory_histories ih on
			ih.sku_id = sl.sku_id
			and ih.in_tx_id = sl.in_tx_id
		left join public.inv_transactions it on it.id = ih.tx_id 

		where
			ih.tx_id is not null
			and ih.in_tx_id != ih.tx_id
		`,
		graph.DependName(o, stock.InboundSkuLog{}),
	)
}

// TableName implements [batch_compute.Table].
func (o OutboundSkuLog) TableName() string {
	return "outbound_sku_log"
}

// Temporary implements [batch_compute.Table].
func (o OutboundSkuLog) Temporary() bool {
	return true
}

type InboundSkuOverflow struct{}

// BuildQuery implements [batch_compute.Table].
func (i InboundSkuOverflow) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with d as (
			select 
				os.created,
				os.in_tx_id,
				os.sku_id,
				(os.out_item_count * -1) as item_count
			from %s os
			
			union all
			
			select 
				sl.created,
				sl.in_tx_id,
				sl.sku_id,
				sl.item_count
			from %s sl
		)

		select 
			d.*,
			sum(d.item_count) over (
				partition by (d.in_tx_id, d.sku_id)
				order by d.created asc
			) as overflow_count
		from d
		order by d.created desc		
		`,
		graph.DependName(i, OutboundSkuLog{}),
		graph.DependName(i, stock.InboundSkuLog{}),
	)
}

// TableName implements [batch_compute.Table].
func (i InboundSkuOverflow) TableName() string {
	return "inbound_sku_overflow"
}

// Temporary implements [batch_compute.Table].
func (i InboundSkuOverflow) Temporary() bool {
	return true
}
