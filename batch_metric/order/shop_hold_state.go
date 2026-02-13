package order

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type ShopHoldState struct{}

// BuildQuery implements [batch_compute.Table].
func (s ShopHoldState) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
	select 
		coh.shop_id,
		sum(coh.tx_count) as tx_count,
		sum(coh.revenue_amount) as revenue_amount
	from %s coh
	group by coh.shop_id
	`,
		graph.DependName(CurrentOrderHold{}),
	)
}

// TableName implements batch_compute.Table.
func (s ShopHoldState) TableName() string {
	return "shop_hold_state"
}

// Temporary implements batch_compute.Table.
func (s ShopHoldState) Temporary() bool {
	return false
}
