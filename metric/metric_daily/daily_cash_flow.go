package metric_daily

import (
	"fmt"

	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
)

func CashFlow(kv stream_core.KeyStore) stream_utils.ChainNextHandler[*accounting_core.JournalEntry] {
	return func(next stream_utils.ChainNextFunc[*accounting_core.JournalEntry]) stream_utils.ChainNextFunc[*accounting_core.JournalEntry] {
		return func(entry *accounting_core.JournalEntry) error {
			switch entry.Account.AccountKey {
			case accounting_core.CashAccount, accounting_core.ShopeepayAccount:
			default:
				return next(entry)
			}

			key := fmt.Sprintf(
				"daily/%s/team/%d/account/%s",
				entry.EntryTime.Format("2006-01-02"),
				entry.TeamID,
				entry.Account.AccountKey,
			)

			kv.IncFloat64(key+"/debit", entry.Debit)
			kv.IncFloat64(key+"/credit", entry.Credit)

			switch entry.Account.BalanceType {
			case accounting_core.DebitBalance:
				kv.PutFloat64(key+"/balance",
					kv.GetFloat64(key+"/debit")-kv.GetFloat64(key+"/credit"),
				)
			case accounting_core.CreditBalance:
				kv.PutFloat64(key+"/balance",
					kv.GetFloat64(key+"/credit")-kv.GetFloat64(key+"/debit"),
				)
			}

			return next(entry)
		}
	}
}
