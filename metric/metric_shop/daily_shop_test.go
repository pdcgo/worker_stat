package metric_shop_test

import (
	"log"
	"testing"
	"time"

	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/pdcgo/worker_stat/batch_model"
	"github.com/pdcgo/worker_stat/metric/metric_shop"
	"github.com/pdcgo/worker_stat/writer"
	"github.com/wargasipil/stream_engine/stream_mock"
	"github.com/wargasipil/stream_engine/stream_utils"
	"github.com/zeebo/assert"
)

func TestIterateDate(t *testing.T) {
	metric_shop.EachDayFn(time.Now().AddDate(0, 0, -5), time.Now(), func(ti time.Time) {
		key := ti.Format("2006-01-02")
		t.Log(key)
	})
}

func TestCalculateMetric(t *testing.T) {

	mockWriter := func(data writer.ItemWriter) error {
		log.Println(data.GetKey(), data.Any())
		return nil
	}

	store := stream_mock.NewInMemoryKeyStore()

	handler := stream_utils.NewChain(
		metric_shop.DailyShopCalculate(store, mockWriter),
	)

	err := handler(&batch_model.BatchJournalEntry{
		Entries: []*accounting_core.JournalEntry{

			{
				TransactionID: 1,
				TeamID:        1,
				Account: &accounting_core.Account{
					AccountKey:  accounting_core.SellingReceivableAccount,
					BalanceType: accounting_core.DebitBalance,
				},
				EntryTime: time.Now().AddDate(0, 0, -2),
				Debit:     250,
			},
			{
				TransactionID: 2,
				TeamID:        1,
				Account: &accounting_core.Account{
					AccountKey:  accounting_core.SellingReceivableAccount,
					BalanceType: accounting_core.DebitBalance,
				},
				EntryTime: time.Now().AddDate(0, 0, -1),
				Debit:     250,
			},
			{
				TransactionID: 1,
				TeamID:        1,
				Account: &accounting_core.Account{
					AccountKey:  accounting_core.SellingReceivableAccount,
					BalanceType: accounting_core.DebitBalance,
				},
				EntryTime: time.Now().AddDate(0, 0, -5),
				Debit:     1,
			},
			{
				TransactionID: 3,
				TeamID:        1,
				Account: &accounting_core.Account{
					AccountKey:  accounting_core.SellingReceivableAccount,
					BalanceType: accounting_core.DebitBalance,
				},
				EntryTime: time.Now(),
				Debit:     140,
			},
		},
		Shop: map[uint]*accounting_core.TransactionShop{
			1: {
				ShopID: 1,
			},
			2: {
				ShopID: 1,
			},
			3: {
				ShopID: 1,
			},
		},
	})

	met := metric_shop.NewMetricDailyShop(store, time.Now().Format("2006-01-02"), 1, string(accounting_core.SellingReceivableAccount))
	assert.Equal(t, float64(641), met.GetLastBalance())

	assert.Nil(t, err)

}
