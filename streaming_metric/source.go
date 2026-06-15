package streaming_metric

import (
	"github.com/pdcgo/shared/db_models"
)

type InvTransactionChange struct {
	At     int64 `gorm:"primarykey"`
	TxID   uint64
	TxType db_models.InvTxType
	// ModType replication.ModificationType
	Status db_models.InvTxStatus
}

// Temporary implements [streaming_compute.StreamingSource].
func (i *InvTransactionChange) Temporary() bool {
	return false
}

// IsSource implements [streaming_compute.StreamingSource].
func (i *InvTransactionChange) IsSource() bool {
	return true
}

// StreamTableName implements [streaming_compute.StreamingSource].
func (i *InvTransactionChange) StreamTableName() string {
	return "inv_transaction_change"
}
