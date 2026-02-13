package accounting

import (
	"fmt"

	"github.com/pdcgo/worker_stat/batch_compute"
)

type TeamReceivable struct{}

// CreateQuery implements batch_compute.Table.
func (r TeamReceivable) CreateQuery(schema batch_compute.Schema) string {
	return `
	with recv as (
		select 
			je.team_id as team_id,
			date(je.entry_time) as day,
			sum(je.debit) as debit,
			sum(je.credit) as credit,
			sum(je.debit - je.credit) as ch_balance
			
		from journal_entries je
		join accounts a on a.id = je.account_id
		where a.account_key = 'selling_receivable'
		group by (
			day,
			je.team_id
		)
	)
	
	select 
		recv.*,
		sum(recv.ch_balance) over (
			partition by recv.team_id
			order by recv.day asc
		) as balance
	from recv
	`
}

// DependsTable implements batch_compute.Table.
func (r TeamReceivable) DependsTable() []batch_compute.Table {
	return []batch_compute.Table{}
}

// TableName implements batch_compute.Table.
func (r TeamReceivable) TableName() string {
	return "team_receivable"
}

// Temporary implements batch_compute.Table.
func (r TeamReceivable) Temporary() bool {
	return false
}

type ShopReceivable struct{}

// BuildQuery implements [batch_compute.Table].
func (s *ShopReceivable) BuildQuery(graph *batch_compute.GraphContext) string {
	panic("unimplemented")
}

// CreateQuery implements batch_compute.Table.
func (s ShopReceivable) CreateQuery(schema batch_compute.Schema) string {
	return `
	with recv as (
		select 
			ts.shop_id as shop_id,
			date(je.entry_time) as day,
			sum(je.debit) as debit,
			sum(je.credit) as credit,
			sum(je.debit - je.credit) as ch_balance
			
		from journal_entries je
		join accounts a on a.id = je.account_id
		join transaction_shops ts on ts.transaction_id = je.transaction_id
		where a.account_key = 'selling_receivable'
		group by (
			day,
			ts.shop_id
		)
	)
	
	select 
		recv.*,
		sum(recv.ch_balance) over (
			partition by recv.shop_id
			order by recv.day asc
		) as balance
	from recv
	`
}

// DependsTable implements batch_compute.Table.
func (s ShopReceivable) DependsTable() []batch_compute.Table {
	return []batch_compute.Table{}
}

// TableName implements batch_compute.Table.
func (s ShopReceivable) TableName() string {
	return "shop_receivable"
}

// Temporary implements batch_compute.Table.
func (s ShopReceivable) Temporary() bool {
	return false
}

type ShopReceivableErr struct{}

// CreateQuery implements batch_compute.Table.
func (s ShopReceivableErr) CreateQuery(schema batch_compute.Schema) string {
	return fmt.Sprintf(
		`
		select 
			sr.shop_id,
			sr.day,
			(sr.balance - dsh.hold_count) as hold_amount_err 
		from %s sr 
		left join stats.daily_shop_holds dsh on 
			dsh.shop_id = sr.shop_id 
			and dsh.day = sr.day
	`,
		schema.GetTableName(&ShopReceivable{}),
	)
}

// DependsTable implements batch_compute.Table.
func (s ShopReceivableErr) DependsTable() []batch_compute.Table {
	return []batch_compute.Table{}
}

// TableName implements batch_compute.Table.
func (s ShopReceivableErr) TableName() string {
	return "shop_receivable_err"
}

// Temporary implements batch_compute.Table.
func (s ShopReceivableErr) Temporary() bool {
	return false
}
