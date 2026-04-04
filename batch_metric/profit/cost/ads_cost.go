package cost

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type TeamAdsCost struct{}

func (t TeamAdsCost) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select
			date(ah.at) as day,
			ah.team_id,
			
			sum(ah.amount) as ads_expense_amount
			
		from public.ads_expense_histories ah
		where ah.at > '%s'
		group by (day, ah.team_id)  
		`,
		graph.Filter.StartDate.Format("2006-01-02"),
	)
}

func (t TeamAdsCost) TableName() string {
	return "team_ads_cost"
}

func (t TeamAdsCost) Temporary() bool {
	return true
}

type ShopAdsCost struct{}

func (t ShopAdsCost) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select
			date(ah.at) as day,
			ah.marketplace_id,
			
			sum(ah.amount) as ads_expense_amount
			
		from public.ads_expense_histories ah
		where ah.at > '%s'
		group by (day, ah.marketplace_id)  
		`,
		graph.Filter.StartDate.Format("2006-01-02"),
	)
}

func (t ShopAdsCost) TableName() string {
	return "shop_ads_cost"
}

func (t ShopAdsCost) Temporary() bool {
	return true
}

type UserAdsCost struct{}

func (t UserAdsCost) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select
			date(ah.at) as day,
			ah.created_by_id,
			
			sum(ah.amount) as ads_expense_amount
			
		from public.ads_expense_histories ah
		where ah.at > '%s'
		group by (day, ah.created_by_id)  
		`,
		graph.Filter.StartDate.Format("2006-01-02"),
	)
}

func (t UserAdsCost) TableName() string {
	return "user_ads_cost"
}

func (t UserAdsCost) Temporary() bool {
	return true
}
