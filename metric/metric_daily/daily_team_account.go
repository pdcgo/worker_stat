package metric_daily

import (
	"fmt"

	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/pdcgo/worker_stat/metric/metric_key"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
)

func DailyCashFlowAccount(kv *stream_core.HashMapCounter) stream_utils.ChainNextHandler[*accounting_core.JournalEntry] {
	return func(next stream_utils.ChainNextFunc[*accounting_core.JournalEntry]) stream_utils.ChainNextFunc[*accounting_core.JournalEntry] {

		return func(entry *accounting_core.JournalEntry) error {

			switch entry.Account.AccountKey {
			case accounting_core.CashAccount, accounting_core.ShopeepayAccount:
			default:
				return next(entry)
			}

			err := kv.Transaction(func(tx *stream_core.Transaction) error {

				prefix := metric_key.NewDailyTeamPrefix(entry.EntryTime, uint64(entry.TeamID))
				tx.IncFloat64(prefix.Join("all_cash_account/debit"), entry.Debit)
				tx.IncFloat64(prefix.Join("all_cash_account/credit"), entry.Credit)

				tx.PutFloat64(prefix.Join("all_cash_account/balance"),
					tx.GetFloat64(prefix.Join("all_cash_account/debit"))-tx.GetFloat64(prefix.Join("all_cash_account/credit")),
				)

				return nil
			})

			if err != nil {
				return err
			}

			return next(entry)
		}
	}
}

func DailyStockAccount(kv *stream_core.HashMapCounter) stream_utils.ChainNextHandler[*accounting_core.JournalEntry] {
	return func(next stream_utils.ChainNextFunc[*accounting_core.JournalEntry]) stream_utils.ChainNextFunc[*accounting_core.JournalEntry] {

		return func(entry *accounting_core.JournalEntry) error {

			switch entry.Account.AccountKey {
			case accounting_core.StockPendingAccount,
				accounting_core.StockReadyAccount,
				accounting_core.StockLostAccount,
				accounting_core.StockBrokenAccount:
			default:
				return next(entry)
			}

			err := kv.Transaction(func(tx *stream_core.Transaction) error {

				prefix := metric_key.NewDailyTeamPrefix(entry.EntryTime, uint64(entry.TeamID))
				tx.IncFloat64(prefix.Join("all_stock/debit"), entry.Debit)
				tx.IncFloat64(prefix.Join("all_stock/credit"), entry.Credit)

				tx.PutFloat64(prefix.Join("all_stock/balance"),
					tx.GetFloat64(prefix.Join("all_stock/debit"))-tx.GetFloat64(prefix.Join("all_stock/credit")),
				)

				return nil
			})

			if err != nil {
				return err
			}

			return next(entry)
		}
	}
}

func DailyTeamToTeamAccount(kv *stream_core.HashMapCounter) stream_utils.ChainNextHandler[*accounting_core.JournalEntry] {
	return func(next stream_utils.ChainNextFunc[*accounting_core.JournalEntry]) stream_utils.ChainNextFunc[*accounting_core.JournalEntry] {
		return func(entry *accounting_core.JournalEntry) error {
			// var journalTeamID, accountTeamID uint64
			// journalTeamID = uint64(entry.TeamID)
			// accountTeamID = uint64(entry.Account.TeamID)

			err := kv.Transaction(func(tx *stream_core.Transaction) error {
				key := fmt.Sprintf(
					"daily/%s/team/%d/account/%s/team/%d",
					entry.EntryTime.Format("2006-01-02"),
					entry.TeamID,
					entry.Account.AccountKey,
					entry.Account.TeamID,
				)

				accountKey := fmt.Sprintf(
					"daily/%s/team/%d/account/%s",
					entry.EntryTime.Format("2006-01-02"),
					entry.TeamID,
					entry.Account.AccountKey,
				)

				tx.IncFloat64(key+"/debit", entry.Debit)
				tx.IncFloat64(key+"/credit", entry.Credit)
				tx.IncFloat64(accountKey+"/debit", entry.Debit)
				tx.IncFloat64(accountKey+"/credit", entry.Credit)

				switch entry.Account.BalanceType {
				case accounting_core.DebitBalance:
					tx.PutFloat64(key+"/balance",
						tx.GetFloat64(key+"/debit")-tx.GetFloat64(key+"/credit"),
					)

					tx.PutFloat64(accountKey+"/balance",
						tx.GetFloat64(accountKey+"/debit")-tx.GetFloat64(accountKey+"/credit"),
					)

				case accounting_core.CreditBalance:

					tx.PutFloat64(key+"/balance",
						tx.GetFloat64(key+"/credit")-tx.GetFloat64(key+"/debit"),
					)

					tx.PutFloat64(accountKey+"/balance",
						tx.GetFloat64(accountKey+"/credit")-tx.GetFloat64(accountKey+"/debit"),
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
					tx.IncFloat64(key+"/balance", balance)
					tx.IncFloat64(key+"/debit", entry.Debit)
					tx.IncFloat64(key+"/credit", entry.Credit)

				case accounting_core.LIABILITY:
					balance := entry.Credit - entry.Debit
					key := fmt.Sprintf(
						"daily/%s/team/%d/liability",
						entry.EntryTime.Format("2006-01-02"),
						entry.TeamID,
					)
					tx.IncFloat64(key+"/balance", balance)
					tx.IncFloat64(key+"/debit", entry.Debit)
					tx.IncFloat64(key+"/credit", entry.Credit)

				case accounting_core.EQUITY:
					balance := entry.Credit - entry.Debit
					key := fmt.Sprintf(
						"daily/%s/team/%d/equity",
						entry.EntryTime.Format("2006-01-02"),
						entry.TeamID,
					)
					tx.IncFloat64(key+"/balance", balance)
					tx.IncFloat64(key+"/debit", entry.Debit)
					tx.IncFloat64(key+"/credit", entry.Credit)

				case accounting_core.EXPENSE:
					balance := entry.Debit - entry.Credit
					key := fmt.Sprintf(
						"daily/%s/team/%d/expense",
						entry.EntryTime.Format("2006-01-02"),
						entry.TeamID,
					)
					tx.IncFloat64(key+"/balance", balance)
					tx.IncFloat64(key+"/debit", entry.Debit)
					tx.IncFloat64(key+"/credit", entry.Credit)

				case accounting_core.REVENUE:
					balance := entry.Credit - entry.Debit
					key := fmt.Sprintf(
						"daily/%s/team/%d/revenue",
						entry.EntryTime.Format("2006-01-02"),
						entry.TeamID,
					)
					tx.IncFloat64(key+"/balance", balance)
					tx.IncFloat64(key+"/debit", entry.Debit)
					tx.IncFloat64(key+"/credit", entry.Credit)
				}

				dailyKey := fmt.Sprintf(
					"daily/%s/team/%d",
					entry.EntryTime.Format("2006-01-02"),
					entry.TeamID,
				)

				tx.PutFloat64(dailyKey+"/error_balance",
					kv.GetFloat64(dailyKey+"/asset/balance")-kv.GetFloat64(dailyKey+"/liability/balance")-kv.GetFloat64(dailyKey+"/equity/balance"),
				)

				tx.PutFloat64(dailyKey+"/gross_profit",
					kv.GetFloat64(dailyKey+"/revenue/balance")-kv.GetFloat64(dailyKey+"/expense/balance"),
				)

				return nil
			})

			if err != nil {
				return err
			}

			return next(entry)
		}
	}
}
