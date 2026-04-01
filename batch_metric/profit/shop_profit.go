package profit

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type ShopOrderRevenue struct{}

func (s ShopOrderRevenue) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			date(al.fund_at) as day,
			al.shop_id,
			sum(al.amount) as total_revenue_amount,
			
			sum(al.real_revenue_amount) as real_revenue_amount,
			sum(al.estimated_revenue_amount) as estimated_revenue_amount,
			
			sum(al.adj_real_revenue_amount) as adj_real_revenue_amount,
			
			
			sum(al.additional_order_count) as additional_order_count,
			sum(al.additional_revenue_amount) as additional_revenue_amount
		from %s al
		group by (
			day,
			al.shop_id
		)
		`,
		graph.DependName(s, AdjustmentLog{}),
	)
}

func (s ShopOrderRevenue) TableName() string {
	return "shop_order_revenue"
}

func (s ShopOrderRevenue) Temporary() bool {
	return false
}
