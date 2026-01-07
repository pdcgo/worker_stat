package metric_team

import (
	"fmt"

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

			err := kv.Transaction(func(tx *stream_core.Transaction) error {
				tx.IncFloat64(key+"/debit", entry.Debit)
				tx.IncFloat64(key+"/credit", entry.Credit)

				// log.Println(key + "/credit")

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

				acc := entry.Account

				switch acc.Coa {
				case accounting_core.ASSET:
					balance := entry.Debit - entry.Credit
					key := fmt.Sprintf(
						"team/%d/asset",
						entry.TeamID,
					)
					tx.IncFloat64(key+"/balance", balance)
					tx.IncFloat64(key+"/debit", entry.Debit)
					tx.IncFloat64(key+"/credit", entry.Credit)

				case accounting_core.LIABILITY:
					balance := entry.Credit - entry.Debit
					key := fmt.Sprintf(
						"team/%d/liability",
						entry.TeamID,
					)
					tx.IncFloat64(key+"/balance", balance)
					tx.IncFloat64(key+"/debit", entry.Debit)
					tx.IncFloat64(key+"/credit", entry.Credit)

				case accounting_core.EXPENSE:
					balance := entry.Debit - entry.Credit
					key := fmt.Sprintf(
						"team/%d/expense",
						entry.TeamID,
					)
					tx.IncFloat64(key+"/balance", balance)
					tx.IncFloat64(key+"/debit", entry.Debit)
					tx.IncFloat64(key+"/credit", entry.Credit)

				case accounting_core.REVENUE:
					balance := entry.Credit - entry.Debit
					key := fmt.Sprintf(
						"team/%d/revenue",
						entry.TeamID,
					)
					tx.IncFloat64(key+"/balance", balance)
					tx.IncFloat64(key+"/debit", entry.Debit)
					tx.IncFloat64(key+"/credit", entry.Credit)
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
