package metric_shop

import (
	"errors"
	"time"

	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/pdcgo/worker_stat/batch_model"
	"github.com/pdcgo/worker_stat/writer"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
)

//go:generate metric_generate
type DailyShop struct {
	ID          int64   `metric:"id" json:"id" gorm:"primaryKey;autoIncrement:false"`
	Day         string  `metric:"index" json:"day"`
	ShopID      uint64  `metric:"index" json:"shop_id"`
	Account     string  `metric:"index" json:"account"`
	Debit       float64 `json:"debit"`
	Credit      float64 `json:"credit"`
	Balance     float64 `json:"balance"`
	LastBalance float64 `json:"last_balance"`
}

type ShopLastBalanceDate struct {
	ID          int64  `metric:"id" json:"id" gorm:"primaryKey;autoIncrement:false"`
	ShopID      uint64 `metric:"index" json:"shop_id"`
	Account     string `metric:"index" json:"account"`
	LastUpdated int64  `json:"last_updated"`
}

func DailyShopCalculate(
	kv stream_core.KeyStore,
	dataWriter stream_utils.ChainNextFunc[writer.ItemWriter],
) stream_utils.ChainNextHandler[*batch_model.BatchJournalEntry] {
	return func(next stream_utils.ChainNextFunc[*batch_model.BatchJournalEntry]) stream_utils.ChainNextFunc[*batch_model.BatchJournalEntry] {
		return func(batch *batch_model.BatchJournalEntry) error {
			var err error
			for _, entry := range batch.Entries {
				shop := batch.Shop[entry.TransactionID]
				if shop == nil {
					continue
				}

				switch entry.Account.AccountKey {
				case accounting_core.SellingReceivableAccount,
					accounting_core.SalesRevenueAccount:
				default:
					continue
				}

				met := NewMetricDailyShop(kv, entry.EntryTime.Format("2006-01-02"), uint64(shop.ShopID), string(entry.Account.AccountKey))
				met.IncDebit(entry.Debit)
				met.IncCredit(entry.Credit)

				switch entry.Account.BalanceType {
				case accounting_core.DebitBalance:
					met.PutBalance(met.GetDebit() - met.GetCredit())
				case accounting_core.CreditBalance:
					met.PutBalance(met.GetCredit() - met.GetDebit())
				default:
					return errors.New("account balance not specified")
				}

				// calculate last balance
				last := NewMetricShopLastBalanceDate(kv, uint64(shop.ShopID), string(entry.Account.AccountKey))
				curr := entry.EntryTime.UnixMilli()

				var tproc int64
				if last.GetLastUpdated() == 0 {
					tproc = curr
				} else {
					if curr < last.GetLastUpdated() {
						tproc = curr
					} else {
						tproc = last.GetLastUpdated()
					}
				}

				EachDayFn(time.UnixMilli(tproc).AddDate(0, 0, -1), time.Now(), func(t time.Time) {
					prev := t.AddDate(0, 0, -1).Format("2006-01-02")
					current := t.Format("2006-01-02")

					prevmet := NewMetricDailyShop(kv, prev, uint64(shop.ShopID), string(entry.Account.AccountKey))
					curmet := NewMetricDailyShop(kv, current, uint64(shop.ShopID), string(entry.Account.AccountKey))

					curmet.PutLastBalance(prevmet.GetLastBalance() + curmet.GetBalance())

				})

				last.PutLastUpdated(entry.EntryTime.UnixMilli())

				// save to postgress
				err = dataWriter(met)
				if err != nil {
					return err
				}
			}

			return next(batch)
		}
	}
}

func EachDayFn(start, end time.Time, fn func(time.Time)) {

	for d := start; d.Before(end); d = d.AddDate(0, 0, 1) {
		fn(d)
	}
}
