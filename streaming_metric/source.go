package streaming_metric

import (
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/pdcgo/worker_stat/replication"
	"gorm.io/gorm"
)

type InvTransactionChange struct {
	At      int64 `gorm:"primarykey"`
	TxID    uint64
	TxType  db_models.InvTxType
	ModType replication.ModificationType
	Status  db_models.InvTxStatus
}

// AfterCalculate implements [streaming_compute.SourceTable].
func (i *InvTransactionChange) AfterCalculate(db *gorm.DB) error {

	return db.
		Session(&gorm.Session{AllowGlobalUpdate: true}).
		Table("test.inv_transaction_changes").
		Delete(&InvTransactionChange{}).
		Error
}

// BuildQuery implements [batch_compute.Table].
func (i *InvTransactionChange) BuildQuery(graph *batch_compute.GraphContext) string {
	panic("unimplemented")
}

// BuildQuery implements [batch_compute.Table].
func (i *InvTransactionChange) BuildQueries(graph *batch_compute.GraphContext) []string {
	return []string{}
}

// Temporary implements [batch_compute.Table].
func (i *InvTransactionChange) Temporary() bool {
	return true
}

func (*InvTransactionChange) TableName() string {
	return "inv_transaction_changes"
}
