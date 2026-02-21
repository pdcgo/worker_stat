package stock

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type InboundHistory struct{}

// BuildQuery implements [batch_compute.Table].
func (i InboundHistory) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with d as (
			select 
				ih.in_tx_id,
				ih.sku_id,
				sum(ih.count) as item_count,
				sum(ih.count * (ih.price + coalesce(ih.ext_price, 0))) as item_amount
				
			from  public.invertory_histories ih 
			where
				ih.in_tx_id = ih.tx_id
				
			group by (
				ih.in_tx_id,
				ih.sku_id
			)
		)

		select 
			it.type,
			it.created,
			d.* 
		from d
		join public.inv_transactions it on it.id = d.in_tx_id
		where
			it.created > '%s'
	`,
		graph.Filter.StartDate.Format("2006-01-02"),
	)
}

// TableName implements [batch_compute.Table].
func (i InboundHistory) TableName() string {
	return "inbound_history"
}

// Temporary implements [batch_compute.Table].
func (i InboundHistory) Temporary() bool {
	return true
}

type OutboundHistory struct{}

// BuildQuery implements [batch_compute.Table].
func (o OutboundHistory) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
	with d as (
		select 
			ih.in_tx_id,
			ih.tx_id,
			ih.sku_id,
			sum(ih.count) as item_count,
			sum(ih.count * (ih.price + coalesce(ih.ext_price, 0))) as item_amount
			
		from  public.invertory_histories ih 
		where
			ih.tx_id is not null
			and ih.in_tx_id != ih.tx_id
			

		group by (
			ih.in_tx_id,
			ih.tx_id,
			ih.sku_id
		)
	)

	select 
		it.type,
		it.created,
		d.* 
	from d
	left join public.inv_transactions it on it.id = d.tx_id
	`
}

// TableName implements [batch_compute.Table].
func (o OutboundHistory) TableName() string {
	return "outbound_history"
}

// Temporary implements [batch_compute.Table].
func (o OutboundHistory) Temporary() bool {
	return true
}

type InboundSpent struct{}

// BuildQuery implements [batch_compute.Table].
func (i InboundSpent) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with d as (
			select 
				oh.in_tx_id,
				oh.sku_id,
				sum(item_count) as item_count,
				sum(item_amount) as item_amount
			from %s oh
			group by (
				oh.in_tx_id,
				oh.sku_id
			)
		)


		select 
			ih.in_tx_id,
			ih.sku_id,
			ih.created,
			
			ih.item_count as inbound_item_count,
			ih.item_amount as inbound_item_amount,
			
			coalesce(d.item_count, 0) as outbound_item_count,
			coalesce(d.item_amount, 0) as outbound_item_amount,
			
			(ih.item_count - coalesce(d.item_count, 0)) as count_err,
			(ih.item_amount - coalesce(d.item_amount, 0)) as amount_err
			
		from %s ih 
		left join d on 
			d.in_tx_id = ih.in_tx_id
			and d.sku_id = ih.sku_id
		`,
		graph.DependName(i, OutboundHistory{}),
		graph.DependName(i, InboundHistory{}),
	)
}

// TableName implements [batch_compute.Table].
func (i InboundSpent) TableName() string {
	return "inbound_spent"
}

// Temporary implements [batch_compute.Table].
func (i InboundSpent) Temporary() bool {
	return false
}

type InboundSpentNegative struct{}

// BuildQuery implements [batch_compute.Table].
func (i InboundSpentNegative) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select 
			*
		from %s s
		where 
			s.count_err < 0
			or s.amount_err < 0
		`,
		graph.DependName(i, InboundSpent{}),
	)
}

// TableName implements [batch_compute.Table].
func (i InboundSpentNegative) TableName() string {
	return "inbound_spent_negative"
}

// Temporary implements [batch_compute.Table].
func (i InboundSpentNegative) Temporary() bool {
	return false
}
