package metric_team

import (
	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
)

//go:generate metric_generate
type TeamAccount struct {
	ID      int64   `metric:"id" json:"id" gorm:"primaryKey;autoIncrement:false"`
	TeamID  uint64  `metric:"index"`
	Account string  `metric:"index"`
	Debit   float64 `json:"debit"`
	Credit  float64 `json:"credit"`
	Balance float64 `json:"balance"`
}

func TeamAccountFunc(kv *stream_core.HashMapCounter) stream_utils.ChainNextHandler[*accounting_core.JournalEntry] {
	return func(next stream_utils.ChainNextFunc[*accounting_core.JournalEntry]) stream_utils.ChainNextFunc[*accounting_core.JournalEntry] {
		return func(entry *accounting_core.JournalEntry) error {

			err := kv.Transaction(func(tx *stream_core.Transaction) error {
				acc := entry.Account

				metric := NewMetricTeamAccount(tx, uint64(entry.TeamID), string(acc.AccountKey))
				metric.IncDebit(entry.Debit)
				metric.IncCredit(entry.Credit)

				coaMetric := NewMetricTeamAccount(tx, uint64(entry.TeamID), acc.Coa.String())
				coaMetric.IncDebit(entry.Debit)
				coaMetric.IncCredit(entry.Credit)

				switch entry.Account.BalanceType {
				case accounting_core.DebitBalance:
					metric.PutBalance(
						metric.GetDebit() - metric.GetCredit(),
					)

					coaMetric.PutBalance(
						coaMetric.GetDebit() - coaMetric.GetCredit(),
					)

				case accounting_core.CreditBalance:
					metric.PutBalance(
						metric.GetCredit() - metric.GetDebit(),
					)

					coaMetric.PutBalance(
						coaMetric.GetDebit() - coaMetric.GetCredit(),
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
