package profit

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type ShopOrderRevenueInvalid struct{}

// BuildQuery implements [batch_compute.Table].
func (s ShopOrderRevenueInvalid) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with d as (
			select 
				coalesce(dw.day, tor.day) as day,
				coalesce(dw.shop_id, tor.shop_id) as shop_id,
				coalesce(dw.amount, 0) as csv_amount,
				tor.total_revenue_amount
			from %s dw
			full join %s tor on tor.shop_id = dw.shop_id and tor.day = dw.day
		),

		man as (
			select 
			date(al.fund_at) as day,
			al.shop_id,
			sum(al.import_amount) as import_revenue_amount,
			sum(al.manual_amount) as manual_revenue_amount
			from %s al
			group by (day, shop_id)
		)
		
		

		select
			d.day,
			d.shop_id,
			d.total_revenue_amount,
			man.import_revenue_amount,
			man.manual_revenue_amount,
			d.csv_amount,
			(
				d.csv_amount + man.import_revenue_amount
			) as invalid_amount
		from d
		left join man on man.day = d.day and man.shop_id = d.shop_id
		order by d.day desc
		`,
		graph.DependName(s, ShopDailyWithdrawal{}),
		graph.DependName(s, ShopOrderRevenue{}),
		graph.DependName(s, AdjustmentLog{}),
	)
}

// TableName implements [batch_compute.Table].
func (s ShopOrderRevenueInvalid) TableName() string {
	return "shop_order_revenue_invalid"
}

// Temporary implements [batch_compute.Table].
func (s ShopOrderRevenueInvalid) Temporary() bool {
	return false
}
