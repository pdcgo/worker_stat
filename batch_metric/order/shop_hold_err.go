package order

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type dailyLastShopHold struct{}

// CreateQuery implements batch_compute.Table.
func (d dailyLastShopHold) CreateQuery(schema batch_compute.Schema) string {
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
		schema.GetTableName(DailyShopHold{}),
		schema.GetTableName(DailyShopHold{}),
	)
}

// DependsTable implements batch_compute.Table.
func (d dailyLastShopHold) DependsTable() []batch_compute.Table {
	return []batch_compute.Table{
		DailyShopHold{},
	}
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

// CreateQuery implements batch_compute.Table.
func (t ShopHoldErr) CreateQuery(schema batch_compute.Schema) string {
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
		schema.GetTableName(dailyLastShopHold{}),
		schema.GetTableName(ShopHoldState{}),
	)
}

// DependsTable implements batch_compute.Table.
func (t ShopHoldErr) DependsTable() []batch_compute.Table {
	return []batch_compute.Table{
		dailyLastShopHold{},
		ShopHoldState{},
	}
}

// TableName implements batch_compute.Table.
func (t ShopHoldErr) TableName() string {
	return "shop_hold_err"
}

// Temporary implements batch_compute.Table.
func (t ShopHoldErr) Temporary() bool {
	return false
}
