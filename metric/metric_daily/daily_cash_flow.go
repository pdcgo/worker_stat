package metric_daily

import (
	"fmt"

	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
)

func CashFlow(kv *stream_core.HashMapCounter) stream_utils.ChainNextHandler[*accounting_core.JournalEntry] {
	return func(next stream_utils.ChainNextFunc[*accounting_core.JournalEntry]) stream_utils.ChainNextFunc[*accounting_core.JournalEntry] {
		return func(entry *accounting_core.JournalEntry) error {
			switch entry.Account.AccountKey {
			case accounting_core.CashAccount, accounting_core.ShopeepayAccount:
			default:
				return next(entry)
			}

			err := kv.Transaction(func(tx *stream_core.Transaction) error {
				key := fmt.Sprintf(
					"daily/%s/team/%d/account/%s",
					entry.EntryTime.Format("2006-01-02"),
					entry.TeamID,
					entry.Account.AccountKey,
				)

				tx.IncFloat64(key+"/debit", entry.Debit)
				tx.IncFloat64(key+"/credit", entry.Credit)

				switch entry.Account.BalanceType {
				case accounting_core.DebitBalance:
					tx.PutFloat64(key+"/balance",
						tx.GetFloat64(key+"/debit")-tx.GetFloat64(key+"/credit"),
					)
				case accounting_core.CreditBalance:
					tx.PutFloat64(key+"/balance",
						tx.GetFloat64(key+"/credit")-tx.GetFloat64(key+"/debit"),
					)
				}
				return nil
			})

			if err != nil {
				return err
			}

			return next(entry)
		}
	}
}
