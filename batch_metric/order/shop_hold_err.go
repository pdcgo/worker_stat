package order

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type dailyLastShopHold struct{}

// BuildQuery implements [batch_compute.Table].
func (d dailyLastShopHold) BuildQuery(graph *batch_compute.GraphContext) string {
	tableName := graph.DependName(DailyShopHold{})

	return fmt.Sprintf(
		`
		select 
			dsh.*
		from %s dsh
		join (
			select
				max(day) as day,
				shop_id
			from %s dsh
			group by shop_id
		) l on l.day = dsh.day and l.shop_id = dsh.shop_id
		`,
		tableName,
		tableName,
	)
}

// TableName implements batch_compute.Table.
func (d dailyLastShopHold) TableName() string {
	return "last_daily_shop_hold"
}

// Temporary implements batch_compute.Table.
func (d dailyLastShopHold) Temporary() bool {
	return true
}

type ShopHoldErr struct{}

// BuildQuery implements [batch_compute.Table].
func (t ShopHoldErr) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with data as (
			select
				l.shop_id,
				(l.hold_count - c.tx_count) as hold_count_err,
				(l.hold_amount - c.revenue_amount) as hold_amount_err
				
			from %s l
			left join %s c
				on l.shop_id = c.shop_id
		)
		select 
			*,
			now() as sync_at
		from data
		where 
			hold_amount_err != 0 
			or hold_count_err != 0
		`,
		graph.DependName(dailyLastShopHold{}),
		graph.DependName(ShopHoldState{}),
	)
}

// TableName implements batch_compute.Table.
func (t ShopHoldErr) TableName() string {
	return "shop_hold_err"
}

// Temporary implements batch_compute.Table.
func (t ShopHoldErr) Temporary() bool {
	return false
}
