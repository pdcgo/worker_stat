package metric_daily

import (
	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/pdcgo/worker_stat/metric/metric_key"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
)

//go:generate metric_generate
type DailyTeamAccount struct {
	ID      int64   `metric:"id" json:"id" gorm:"primaryKey;autoIncrement:false"`
	Day     string  `metric:"index" json:"day"`
	TeamID  uint64  `metric:"index" json:"team_id"`
	Account string  `metric:"index" json:"account"`
	Debit   float64 `json:"debit"`
	Credit  float64 `json:"credit"`
	Balance float64 `json:"balance"`
}

type DailyTeamToTeamAccount struct {
	ID       int64   `metric:"id" json:"id" gorm:"primaryKey;autoIncrement:false"`
	Day      string  `metric:"index" json:"day"`
	TeamID   uint64  `metric:"index" json:"team_id"`
	Account  string  `metric:"index" json:"account"`
	ToTeamID uint64  `metric:"index" json:"to_team_id"`
	Debit    float64 `json:"debit"`
	Credit   float64 `json:"credit"`
	Balance  float64 `json:"balance"`
}

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

func DailyTeamToTeamAccountFunc(kv *stream_core.HashMapCounter) stream_utils.ChainNextHandler[*accounting_core.JournalEntry] {
	return func(next stream_utils.ChainNextFunc[*accounting_core.JournalEntry]) stream_utils.ChainNextFunc[*accounting_core.JournalEntry] {
		return func(entry *accounting_core.JournalEntry) error {
			// var journalTeamID, accountTeamID uint64
			// journalTeamID = uint64(entry.TeamID)
			// accountTeamID = uint64(entry.Account.TeamID)

			err := kv.Transaction(func(tx *stream_core.Transaction) error {
				accountMetric := NewMetricDailyTeamAccount(tx, entry.EntryTime.Format("2006-01-02"), uint64(entry.TeamID), string(entry.Account.AccountKey))
				accountToTeamMetric := NewMetricDailyTeamToTeamAccount(
					tx,
					entry.EntryTime.Format("2006-01-02"),
					uint64(entry.TeamID),
					string(entry.Account.AccountKey),
					uint64(entry.Account.TeamID),
				)

				accountMetric.IncDebit(entry.Debit)
				accountMetric.IncCredit(entry.Credit)
				accountToTeamMetric.IncDebit(entry.Debit)
				accountToTeamMetric.IncCredit(entry.Credit)

				switch entry.Account.BalanceType {
				case accounting_core.DebitBalance:
					accountMetric.PutBalance(
						accountMetric.GetDebit() - accountMetric.GetCredit(),
					)

					accountToTeamMetric.PutBalance(
						accountToTeamMetric.GetDebit() - accountToTeamMetric.GetCredit(),
					)

				case accounting_core.CreditBalance:
					accountMetric.PutBalance(
						accountMetric.GetCredit() - accountMetric.GetDebit(),
					)

					accountToTeamMetric.PutBalance(
						accountToTeamMetric.GetCredit() - accountToTeamMetric.GetDebit(),
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
