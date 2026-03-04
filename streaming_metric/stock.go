package streaming_metric

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type SkuReadyStock struct{}

// BuildQuery implements [batch_compute.Table].
func (s SkuReadyStock) BuildQuery(graph *batch_compute.GraphContext) string {

	return fmt.Sprintf(
		`
		with skus as (
			select 
				distinct iti.sku_id as sku_id
			from %s itc 
			left join public.inv_tx_items iti on iti.inv_transaction_id = itc.tx_id
			where 
				itc.tx_type = 'order'
				and (
					itc.mod_type = 'insert'
					or itc.status = 'cancel'
				)
				
		),

		data as (
			select
				s.sku_id as sku_id,
				sum(ih.count * -1) as ready_stock_count,
				sum((ih.count * -1) * (ih.price + coalesce(ih.ext_price, 0))) as ready_stock_amount
			from skus s
			left join public.invertory_histories ih on ih.sku_id = s.sku_id
			where
				ih.tx_id is null
			group by (
				s.sku_id
			)
		)

		select 
			srs.*,
			s.variant_id,
			s.product_id,
			s.team_id 
		from data srs 
		left join public.skus s on srs.sku_id = s.id

		
		`,
		graph.DependName(s, &InvTransactionChange{}),
	)
}

// TableName implements [batch_compute.Table].
func (s SkuReadyStock) TableName() string {
	return "sku_ready_stock"
}

// Temporary implements [batch_compute.Table].
func (s SkuReadyStock) Temporary() bool {
	return true
}

type SkuOngoingStock struct{}

// BuildQuery implements [batch_compute.Table].
func (s SkuOngoingStock) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select
			iti.sku_id,
			count(it.id) filter (where it.status not in ('cancel', 'completed')) ongoing_tx_count,
			sum(iti.count) filter (where it.status not in ('cancel', 'completed')) ongoing_item_count,
			sum(iti.total) filter (where it.status not in ('cancel', 'completed')) ongoing_item_amount

		from %s itc 
		left join public.inv_transactions it on it.id = itc.tx_id 
		left join public.inv_tx_items iti on iti.inv_transaction_id = it.id
		where
			it.type in ('restock', 'return')
		group by
			sku_id
		`,
		graph.DependName(s, &InvTransactionChange{}),
	)
}

// TableName implements [batch_compute.Table].
func (s SkuOngoingStock) TableName() string {
	return "sku_ongoing_stock"
}

// Temporary implements [batch_compute.Table].
func (s SkuOngoingStock) Temporary() bool {
	return true
}

// var cc batch_compute.Table = SkuOngoingStock{}
