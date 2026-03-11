package streaming_metric

import (
	"fmt"

	"github.com/pdcgo/worker_stat/streaming_compute"
)

type SkuReadyStock struct {
	SkuId            string
	ReadyStockCount  int64
	ReadyStockAmount float64
	VariantId        int64
	ProductId        int64
	TeamId           int64
}

// BuildQueries implements [streaming_compute.StreamingTable].
func (sk *SkuReadyStock) BuildQueries(s *streaming_compute.StreamingContext) []string {
	return []string{
		s.InsertOps(
			sk,

			fmt.Sprintf(
				`
				with skus as (
					select
						distinct iti.sku_id as sku_id
					from %s itc
					left join %s iti on iti.inv_transaction_id = itc.tx_id
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
					left join %s ih on ih.sku_id = s.sku_id
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
				s.DependSource(sk, &InvTransactionChange{}),
				s.DependExternal(sk, "public.inv_tx_items"),
				s.DependExternal(sk, "public.invertory_histories"),
			),
		),
	}
}

// StreamTableName implements [streaming_compute.StreamingTable].
func (s *SkuReadyStock) StreamTableName() string {
	return "sku_ready_stock"
}

// Temporary implements [streaming_compute.StreamingTable].
func (s *SkuReadyStock) Temporary() bool {
	return true
}

type SkuOngoingStock struct {
	SkuId             string
	OngoingTxCount    int64
	OngoingItemCount  int64
	OngoingItemAmount float64
	// VariantId        int64
	// ProductId        int64
	// TeamId           int64
}

// BuildQueries implements [streaming_compute.StreamingTable].
func (sk *SkuOngoingStock) BuildQueries(s *streaming_compute.StreamingContext) []string {
	return []string{
		s.InsertOps(
			sk,

			fmt.Sprintf(
				`
				select
					iti.sku_id,
					count(it.id) filter (where it.status not in ('cancel', 'completed')) ongoing_tx_count,
					sum(iti.count) filter (where it.status not in ('cancel', 'completed')) ongoing_item_count,
					sum(iti.total) filter (where it.status not in ('cancel', 'completed')) ongoing_item_amount

				from %s itc
				left join %s it on it.id = itc.tx_id
				left join %s iti on iti.inv_transaction_id = it.id
				where
					it.type in ('restock', 'return')
				group by
					sku_id

				`,
				s.DependSource(sk, &InvTransactionChange{}),
				s.DependExternal(sk, "public.inv_transactions"),
				s.DependExternal(sk, "public.inv_tx_items"),
			),
		),
	}
}

// StreamTableName implements [streaming_compute.StreamingTable].
func (sk *SkuOngoingStock) StreamTableName() string {
	return "sku_ongoing_stock"
}

// Temporary implements [streaming_compute.StreamingTable].
func (sk *SkuOngoingStock) Temporary() bool {
	return true
}
