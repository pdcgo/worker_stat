package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type InboundSkuLog struct{}

// BuildQuery implements [batch_compute.Table].
func (i InboundSkuLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			it.created,
			ih.in_tx_id,
			ih.sku_id,
			ih.count as item_count,
			(ih.price + coalesce(ih.ext_price, 0)) as item_price

		from public.invertory_histories ih 
		left join public.inv_transactions it on it.id = ih.in_tx_id 
		where
			ih.in_tx_id = ih.tx_id
			and it.created > '%s'
		`,
		graph.Filter.StartDate.Format("2006-01-02"),
	)
}

// TableName implements [batch_compute.Table].
func (i InboundSkuLog) TableName() string {
	return "inbound_sku_log"
}

// Temporary implements [batch_compute.Table].
func (i InboundSkuLog) Temporary() bool {
	return true
}

type InvHistoryLog struct{}

// BuildQuery implements [batch_compute.Table].
func (i InvHistoryLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
	select '1'
	`
}

// TableName implements [batch_compute.Table].
func (i InvHistoryLog) TableName() string {
	return "inv_history_log"
}

// Temporary implements [batch_compute.Table].
func (i InvHistoryLog) Temporary() bool {
	return true
}
