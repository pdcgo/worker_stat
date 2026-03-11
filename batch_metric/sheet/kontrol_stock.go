package sheet

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/pdcgo/worker_stat/batch_metric/product"
	"github.com/pdcgo/worker_stat/batch_metric/warehouse"
)

type KontrolStock struct{}

// BuildQuery implements [batch_compute.Table].
func (k KontrolStock) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with s7 as (
			select
				vs.variation_id,
				sum(vs.item_count) filter (where vs.warehouse_id = 94) as palem_sold_count,
				sum(vs.item_count) filter (where vs.warehouse_id = 96) as cemara_sold_count,
				sum(vs.item_count) filter (where vs.warehouse_id = 67) as febri_sold_count,
				sum(vs.item_count) filter (where vs.warehouse_id = 38) as santo_sold_count
				
				
			from %s vs
			where 
				vs.day >= now() - interval '7 days'
			
			group by vs.variation_id

		),

		rd as (
			select 
				vcs.variant_id as variation_id,
				sum(vcs.item_count) filter (where vcs.warehouse_id = 94) as palem_ready_stock_count,
				sum(vcs.item_count) filter (where vcs.warehouse_id = 96) as cemara_ready_stock_count,
				sum(vcs.item_count) filter (where vcs.warehouse_id = 67) as febri_ready_stock_count,
				sum(vcs.item_count) filter (where vcs.warehouse_id = 38) as santo_ready_stock_count
				
			from %s vcs 
			group by vcs.variant_id

		),

		og as (
			select 
				os.variation_id,
				sum(os.item_count) filter (where os.warehouse_id = 94) as palem_ongoing_stock_count,
				sum(os.item_count) filter (where os.warehouse_id = 96) as cemara_ongoing_stock_count,
				sum(os.item_count) filter (where os.warehouse_id = 67) as febri_ongoing_stock_count,
				sum(os.item_count) filter (where os.warehouse_id = 38) as santo_ongoing_stock_count
			from %s os
			group by os.variation_id
		)

		select 
			vv.id as variation_id,
			vr.team_id,
			
			s7.palem_sold_count,
			s7.cemara_sold_count,
			s7.febri_sold_count,
			s7.santo_sold_count,
			
			rd.palem_ready_stock_count,
			rd.cemara_ready_stock_count,
			rd.febri_ready_stock_count,
			rd.santo_ready_stock_count,
			
			og.palem_ongoing_stock_count,
			og.cemara_ongoing_stock_count,
			og.febri_ongoing_stock_count,
			og.santo_ongoing_stock_count
			
		from variation_values vv 
		left join s7 on s7.variation_id = vv.id
		left join rd on rd.variation_id = vv.id
		left join og on og.variation_id = vv.id
		left join %s vr on vr.variation_id = vv.id
		`,
		graph.DependName(k, &product.VariantSold{}),
		graph.DependName(k, &product.VariantCurrentStock{}),
		graph.DependName(k, &warehouse.WarehouseVariantOngoingStock{}),
		graph.DependName(k, &product.VariationRef{}),
	)
}

// TableName implements [batch_compute.Table].
func (k KontrolStock) TableName() string {
	return "kontrol_stock"
}

// Temporary implements [batch_compute.Table].
func (k KontrolStock) Temporary() bool {
	return false
}
