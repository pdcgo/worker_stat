package stock_overflow

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type InboundLog struct{}

// BuildQuery implements [batch_compute.Table].
func (i InboundLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
	select
		ih.in_tx_id,
		ih.sku_id,
		sum(count) filter (where ih.in_tx_id != ih.tx_id ) as out_count,
		sum(count) filter (where ih.in_tx_id = ih.tx_id ) as in_count,
		sum(count*-1) filter (where ih.tx_id is null) as ready_count
	from public.invertory_histories ih
	where 
		ih.in_tx_id is not null
	group by (
		ih.sku_id,
		in_tx_id
	)
	`
}

// TableName implements [batch_compute.Table].
func (i InboundLog) TableName() string {
	return "inbound_log"
}

// Temporary implements [batch_compute.Table].
func (i InboundLog) Temporary() bool {
	return true
}

type InboundOverflow struct{}

// BuildQuery implements [batch_compute.Table].
func (i InboundOverflow) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		with d as (
			select 
				it.created,
				it.type,
				it.warehouse_id,
				d.*,
				(coalesce(d.in_count, 0) - coalesce(d.out_count, 0) - coalesce(d.ready_count, 0)) as overflow
			from %s d
			left join public.inv_transactions it on it.id = d.in_tx_id
		)
		select 
			* 
		from d
		where d.overflow != 0
		`,
		graph.DependName(i, InboundLog{}),
	)
}

// TableName implements [batch_compute.Table].
func (i InboundOverflow) TableName() string {
	return "inbound_overflow"
}

// Temporary implements [batch_compute.Table].
func (i InboundOverflow) Temporary() bool {
	return true
}

type OverflowHaveStock struct{}

// BuildQuery implements [batch_compute.Table].
func (o OverflowHaveStock) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select
			*
		from %s d
		where d.ready_count > 0
		`,
		graph.DependName(o, InboundOverflow{}),
	)
}

// TableName implements [batch_compute.Table].
func (o OverflowHaveStock) TableName() string {
	return "overflow_have_stock"
}

// Temporary implements [batch_compute.Table].
func (o OverflowHaveStock) Temporary() bool {
	return true
}

type OverflowDonthaveStock struct{}

// BuildQuery implements [batch_compute.Table].
func (o OverflowDonthaveStock) BuildQuery(graph *batch_compute.GraphContext) string {
	return fmt.Sprintf(
		`
		select
			*
		from %s d
		where d.ready_count is null or d.ready_count = 0
		`,
		graph.DependName(o, InboundOverflow{}),
	)
}

// TableName implements [batch_compute.Table].
func (o OverflowDonthaveStock) TableName() string {
	return "overflow_donthave_stock"
}

// Temporary implements [batch_compute.Table].
func (o OverflowDonthaveStock) Temporary() bool {
	return true
}

// var cc batch_compute.Table = OverflowDonthaveStock{}
