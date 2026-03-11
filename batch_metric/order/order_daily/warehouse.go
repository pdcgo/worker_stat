package order_daily

import "github.com/pdcgo/worker_stat/batch_compute"

type OrderDailyWarehouse struct{}

// BuildQuery implements [batch_compute.Table].
func (o OrderDailyWarehouse) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
	select '1'
	`
}

// TableName implements [batch_compute.Table].
func (o OrderDailyWarehouse) TableName() string {
	return "order_daily_warehouse"
}

// Temporary implements [batch_compute.Table].
func (o OrderDailyWarehouse) Temporary() bool {
	return false
}

// var _ batch_compute.Table = OrderDailyWarehouse{}

// "name": "Gift Cermin Bulat Mini ",
// "ref_id": "00-0001-V-X-2281-237B",
// "daily_sold_santo": "1327.0",
// "daily_sold_febri": "849.0",
// "daily_sold_palem": "1677.4285714285713",
// "daily_sold_cemara": "1508.1428571428571",
// "stock_ready_santo": "7120",
// "stock_ready_amount_santo": "4011272.3809523811",
// "stock_ready_febri": "4652",
// "stock_ready_amount_febri": "2632385.888888889",
// "stock_ready_palem": "9196",
// "stock_ready_amount_palem": "5261492.111111111",
// "stock_ready_cemara": "9493",
// "stock_ready_amount_cemara": "5429552.861111111",
// "stock_ongoing_santo": "0",
// "stock_ongoing_amount_santo": "0.0",
// "stock_ongoing_febri": "0",
// "stock_ongoing_amount_febri": "0.0",
// "stock_ongoing_palem": "0",
// "stock_ongoing_amount_palem": "0.0",
// "stock_ongoing_cemara": "0",
// "stock_ongoing_amount_cemara": "0.0",
// "markup_in_percent": "20.0"
