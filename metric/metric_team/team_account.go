package metric_team

import (
	"fmt"
	"reflect"

	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
)

func TeamAccount(kv *stream_core.HashMapCounter) stream_utils.ChainNextHandler[*accounting_core.JournalEntry] {
	return func(next stream_utils.ChainNextFunc[*accounting_core.JournalEntry]) stream_utils.ChainNextFunc[*accounting_core.JournalEntry] {
		return func(entry *accounting_core.JournalEntry) error {

			key := fmt.Sprintf(
				"team/%d/account/%s",
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

			acc := entry.Account

			switch acc.Coa {
			case accounting_core.ASSET:
				balance := entry.Debit - entry.Credit
				key := fmt.Sprintf(
					"team/%d/asset",
					entry.TeamID,
				)
				kv.IncFloat64(key, balance)

			case accounting_core.LIABILITY:
				balance := entry.Credit - entry.Debit
				key := fmt.Sprintf(
					"team/%d/liability",
					entry.TeamID,
				)
				kv.IncFloat64(key, balance)

			case accounting_core.EXPENSE:
				balance := entry.Debit - entry.Credit
				key := fmt.Sprintf(
					"team/%d/expense",
					entry.TeamID,
				)
				kv.IncFloat64(key, balance)

			case accounting_core.REVENUE:
				balance := entry.Credit - entry.Debit
				key := fmt.Sprintf(
					"team/%d/revenue",
					entry.TeamID,
				)
				kv.IncFloat64(key, balance)
			}

			return next(entry)
		}
	}
}
