package metric_daily

import (
	"fmt"
	"reflect"

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
				kv.Merge(stream_core.MergeOpMin, reflect.Float64, key+"/balance",
					key+"/debit",
					key+"/credit",
				)
			case accounting_core.CreditBalance:
				kv.Merge(stream_core.MergeOpAdd, reflect.Float64, key+"/balance",
					key+"/credit",
					key+"/debit",
				)
			}

			return next(entry)
		}
	}
}
