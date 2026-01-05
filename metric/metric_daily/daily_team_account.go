package metric_daily

import (
	"fmt"
	"reflect"

	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
)

func DailyTeamAccount(kv *stream_core.HashMapCounter) stream_utils.ChainNextHandler[*accounting_core.JournalEntry] {
	return func(next stream_utils.ChainNextFunc[*accounting_core.JournalEntry]) stream_utils.ChainNextFunc[*accounting_core.JournalEntry] {
		return func(entry *accounting_core.JournalEntry) error {
			// var journalTeamID, accountTeamID uint64
			// journalTeamID = uint64(entry.TeamID)
			// accountTeamID = uint64(entry.Account.TeamID)

			key := fmt.Sprintf(
				"daily/%s/team/%d/account/%s/team/%d",
				entry.EntryTime.Format("2006-01-02"),
				entry.TeamID,
				entry.Account.AccountKey,
				entry.Account.TeamID,
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
					"daily/%s/team/%d/asset",
					entry.EntryTime.Format("2006-01-02"),
					entry.TeamID,
				)
				kv.IncFloat64(key, balance)
				// update general asset
				kv.IncFloat64("all/asset", balance)

			case accounting_core.LIABILITY:
				balance := entry.Credit - entry.Debit
				key := fmt.Sprintf(
					"daily/%s/team/%d/liability",
					entry.EntryTime.Format("2006-01-02"),
					entry.TeamID,
				)
				kv.IncFloat64(key, balance)
				// update general liability
				kv.IncFloat64("all/liability", balance)
			// case accounting_core.EQUITY:
			case accounting_core.EXPENSE:
				balance := entry.Debit - entry.Credit
				key := fmt.Sprintf(
					"daily/%s/team/%d/expense",
					entry.EntryTime.Format("2006-01-02"),
					entry.TeamID,
				)
				kv.IncFloat64(key, balance)
				// update general liability
				kv.IncFloat64("all/expense", balance)

			case accounting_core.REVENUE:
				balance := entry.Credit - entry.Debit
				key := fmt.Sprintf(
					"daily/%s/team/%d/revenue",
					entry.EntryTime.Format("2006-01-02"),
					entry.TeamID,
				)
				kv.IncFloat64(key, balance)
				// update general liability
				kv.IncFloat64("all/revenue", balance)
			}

			return next(entry)
		}
	}
}
