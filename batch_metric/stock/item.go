package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type CreatedItemLog struct{}

// BuildQuery implements [batch_compute.Table].
func (i CreatedItemLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select
			iti.id,
			it.created,
			iti.inv_transaction_id as tx_id,
			it.warehouse_id,
			it.team_id as tx_team_id,
			s.team_id as product_team_id,
			iti.count,
			iti.price,
			iti.total
			
		from public.inv_tx_items iti
		left join public.skus s on s.id = iti.sku_id
		left join public.inv_transactions it on it.id = iti.inv_transaction_id
		where 
			it.created > '%s'
			and it.status != 'cancel'
		
		`,
		graph.Filter.StartDate.Format("2006-01-02"),
	)
}

// TableName implements [batch_compute.Table].
func (i CreatedItemLog) TableName() string {
	return "created_item_log"
}

// Temporary implements [batch_compute.Table].
func (i CreatedItemLog) Temporary() bool {
	return true
}
