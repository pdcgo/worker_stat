package batch_model

import "github.com/pdcgo/accounting_service/accounting_core"

type BatchJournalEntry struct {
	Backfill bool
	Entries  []*accounting_core.JournalEntry
	Shop     map[uint]*accounting_core.TransactionShop
}
