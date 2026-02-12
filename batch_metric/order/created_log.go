package order

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type OrderCreatedLog struct{}

// CreateQuery implements batch_compute.Table.
func (o *OrderCreatedLog) CreateQuery(schema batch_compute.Schema) string {
	return `
	select 
		o.id as order_id,
		o.team_id as team_id,
		o.order_mp_id as shop_id,
		o.created_by_id as user_id,
		o.created_at as at,
		o.order_mp_total as revenue_amount
	from orders o
	where
		o.is_partial != true
		and o.is_order_fake != true
		and o.created_at > '2025-09-09'
	`
}

// DependsTable implements batch_compute.Table.
func (o *OrderCreatedLog) DependsTable() []batch_compute.Table {
	return []batch_compute.Table{}
}

// TableName implements batch_compute.Table.
func (o *OrderCreatedLog) TableName() string {
	return "order_created_log"
}

// Temporary implements batch_compute.Table.
func (o *OrderCreatedLog) Temporary() bool {
	return true
}

type UserRevenueCreated struct{}

// CreateQuery implements batch_compute.Table.
func (u UserRevenueCreated) CreateQuery(schema batch_compute.Schema) string {
	return fmt.Sprintf(
		`
	select 
		date(oc.at) as day,
		oc.user_id,
		count(oc.order_id) as tx_count,
		sum(oc.revenue_amount) as revenue_amount
	from %s oc
	group by (
		day,
		oc.user_id
	)
	`,
		schema.GetTableName(&OrderCreatedLog{}),
	)
}

// DependsTable implements batch_compute.Table.
func (u UserRevenueCreated) DependsTable() []batch_compute.Table {

	return []batch_compute.Table{
		&OrderCreatedLog{},
	}
}

// TableName implements batch_compute.Table.
func (u UserRevenueCreated) TableName() string {
	return "user_revenue_created"
}

// Temporary implements batch_compute.Table.
func (u UserRevenueCreated) Temporary() bool {
	return false
}
