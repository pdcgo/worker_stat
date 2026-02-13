package order

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type ShopCreatedLog struct{}

// BuildQuery implements [batch_compute.Table].
func (t ShopCreatedLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			shop_id,
			date(at) as day,
			count(order_id) as tx_count,
			sum(revenue_amount) as revenue_amount
		from %s
		group by (shop_id, day)
		`,
		graph.DependName(&OrderCreatedLog{}),
	)
}

// TableName implements batch_compute.Table.
func (t ShopCreatedLog) TableName() string {
	return "shop_created_log"
}

// Temporary implements batch_compute.Table.
func (t ShopCreatedLog) Temporary() bool {
	return true
}

type ShopCompletedLog struct{}

// BuildQuery implements [batch_compute.Table].
func (t ShopCompletedLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			shop_id,
			date(at) as day,
			count(order_id) as tx_count,
			sum(revenue_amount) as revenue_amount
		from %s
		group by (shop_id, day)
		`,
		graph.DependName(OrderCompletedLog{}),
	)
}

// TableName implements batch_compute.Table.
func (t ShopCompletedLog) TableName() string {
	return "shop_completed_log"
}

// Temporary implements batch_compute.Table.
func (t ShopCompletedLog) Temporary() bool {
	return true
}

type DailyShopHold struct{}

// BuildQuery implements [batch_compute.Table].
func (d DailyShopHold) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with stage as (
			select 
				COALESCE(cr.shop_id, com.shop_id) shop_id,
				COALESCE(cr.day, com.day) as day,
				COALESCE(com.tx_count, 0) as completed_count,
				COALESCE(com.revenue_amount, 0) as completed_revenue_amount,
				COALESCE(cr.tx_count, 0) as created_count,
				COALESCE(cr.revenue_amount, 0) as created_revenue_amount
			from %s cr
			full join %s com on 
				cr.shop_id = com.shop_id
				and cr.day = com.day
		),
		
		data as (
			select 
				th.*,
				sum(th.created_count - th.completed_count) over (
					partition by th.shop_id
					order by th.day asc) as hold_count,
				sum(th.created_revenue_amount - th.completed_revenue_amount) over (
					partition by th.shop_id
					order by th.day asc) as hold_amount
			from stage th
		)
		
		select 
			*,
			now() as sync_at
		from data
		`,
		graph.DependName(ShopCreatedLog{}),
		graph.DependName(ShopCompletedLog{}),
	)
}

// TableName implements batch_compute.Table.
func (d DailyShopHold) TableName() string {
	return "daily_shop_holds"
}

// Temporary implements batch_compute.Table.
func (d DailyShopHold) Temporary() bool {
	return false
}
