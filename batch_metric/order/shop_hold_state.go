package order

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type ShopHoldState struct{}

// CreateQuery implements batch_compute.Table.
func (s ShopHoldState) CreateQuery(schema batch_compute.Schema) string {
	return fmt.Sprintf(
		`
	select 
		coh.shop_id,
		sum(coh.tx_count) as tx_count,
		sum(coh.revenue_amount) as revenue_amount
	from %s coh
	group by coh.shop_id
	`,
		schema.GetTableName(CurrentOrderHold{}),
	)
}

// DependsTable implements batch_compute.Table.
func (s ShopHoldState) DependsTable() []batch_compute.Table {
	return []batch_compute.Table{
		CurrentOrderHold{},
	}
}

// TableName implements batch_compute.Table.
func (s ShopHoldState) TableName() string {
	return "shop_hold_state"
}

// Temporary implements batch_compute.Table.
func (s ShopHoldState) Temporary() bool {
	return false
}
