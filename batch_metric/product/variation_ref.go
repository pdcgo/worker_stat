package product

import "github.com/pdcgo/worker_stat/batch_compute"

type VariationRef struct{}

// BuildQuery implements [batch_compute.Table].
func (v VariationRef) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
		select
			vv.product_id,
			vv.id as variation_id,
			p.team_id
		from public.variation_values vv 
		left join public.products p on p.id = vv.product_id 
	`
}

// TableName implements [batch_compute.Table].
func (v VariationRef) TableName() string {
	return "variation_ref"
}

// Temporary implements [batch_compute.Table].
func (v VariationRef) Temporary() bool {
	return true
}
