package stock

import "github.com/pdcgo/worker_stat/batch_compute"

type RestockCreatedLog struct{}

// CreateQuery implements batch_compute.Table.
func (r RestockCreatedLog) CreateQuery(schema batch_compute.Schema) string {
	return `
	select
		it.created,
		it.team_id,
		it.warehouse_id,
		rc.payment_type,
		it.total as amount
		
	from public.inv_transactions it
	join public.restock_costs rc on rc.inv_transaction_id = it.id
	where 
		it.created > '2025-09-09'
		and it.type = 'restock'
		and it.status != 'cancel'
	`
}

// DependsTable implements batch_compute.Table.
func (r RestockCreatedLog) DependsTable() []batch_compute.Table {
	return []batch_compute.Table{}
}

// TableName implements batch_compute.Table.
func (r RestockCreatedLog) TableName() string {
	return "restock_created_log"
}

// Temporary implements batch_compute.Table.
func (r RestockCreatedLog) Temporary() bool {
	return true
}

type RestockArrivedLog struct{}

// CreateQuery implements batch_compute.Table.
func (r RestockArrivedLog) CreateQuery(schema batch_compute.Schema) string {
	return `
	select
		it.arrived,
		it.team_id,
		it.warehouse_id,
		rc.payment_type,
		it.total as amount
		
	from public.inv_transactions it
	join public.restock_costs rc on rc.inv_transaction_id = it.id
	where 
		it.created > '2025-09-09'
		and it.type = 'restock'
		and it.status != 'cancel'
	`
}

// DependsTable implements batch_compute.Table.
func (r RestockArrivedLog) DependsTable() []batch_compute.Table {
	return []batch_compute.Table{}
}

// TableName implements batch_compute.Table.
func (r RestockArrivedLog) TableName() string {
	return "restock_arrived_log"
}

// Temporary implements batch_compute.Table.
func (r RestockArrivedLog) Temporary() bool {
	return true
}
