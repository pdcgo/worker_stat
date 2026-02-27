package streaming_metric

import (
	"fmt"
	"time"

	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/pdcgo/worker_stat/streaming_compute"
)

type SkuStock struct {
	SkuID             db_models.SkuID `gorm:"primarykey"`
	ReadyStockCount   int64
	ReadyStockAmount  float64
	OngoingTxCount    int32
	OngoingItemCount  int64
	OngoingItemAmount float64
	LastUpdated       time.Time
}

// BuildQuery implements [batch_compute.Table].
func (s SkuStock) BuildQuery(graph *batch_compute.GraphContext) string {
	panic("unimplemented")
}

func (s SkuStock) BuildQueries(graph *batch_compute.GraphContext) []string {
	insert := streaming_compute.Upsert(
		s.TableName(),
		fmt.Sprintf(
			`
			select *, now() as last_updated from %s
			`,
			graph.DependName(s, SkuReadyStockTemp{}),
		),
		[]string{"sku_id"},
		[]string{"ready_stock_count", "ready_stock_amount", "last_updated"},
	)

	ongoingInsert := streaming_compute.Upsert(
		s.TableName(),
		fmt.Sprintf(
			`
			select *, now() as last_updated from %s
			`,
			graph.DependName(s, SkuOngoingStock{}),
		),
		[]string{"sku_id"},
		[]string{"ongoing_tx_count", "ongoing_item_count", "ongoing_item_amount", "last_updated"},
	)

	return []string{
		insert,
		ongoingInsert,
	}
}

// Temporary implements [batch_compute.Table].
func (s SkuStock) Temporary() bool {
	return true
}

func (SkuStock) TableName() string {
	return "test.sku_stocks"
}
