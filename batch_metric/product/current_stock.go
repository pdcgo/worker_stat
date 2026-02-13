package product

import "github.com/pdcgo/worker_stat/batch_compute"

type VariantOngoingStock struct{}

type VariantCurrentStock struct{}

// BuildQuery implements [batch_compute.Table].
func (v VariantCurrentStock) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
	select
		s.variant_id,
		s.team_id,
		s.warehouse_id,
		sum(ih.count * -1) as item_count,
		sum(-1 * ih.count * (
			ih.price + coalesce(ih.ext_price, 0)
		)) as total_amount
	from invertory_histories ih
	join skus s on s.id = ih.sku_id
	where 
		ih.count < 0
	group by (
		s.variant_id,
		s.team_id,
		s.warehouse_id
	)
	`
}

// TableName implements batch_compute.Table.
func (v VariantCurrentStock) TableName() string {
	return "variant_current_stock"
}

// Temporary implements batch_compute.Table.
func (v VariantCurrentStock) Temporary() bool {
	return false
}
