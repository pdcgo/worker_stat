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

// IsSink implements [streaming_compute.StreamingSink].
func (sk *SkuStock) IsSink() bool {
	return true
}

// BuildQueries implements [streaming_compute.StreamingTable].
func (sk *SkuStock) BuildQueries(s *streaming_compute.StreamingContext) []string {
	return []string{
		s.UpsertOps(&streaming_compute.UpsertPayload{
			DestinationTable: s.TableName(sk),
			Query: fmt.Sprintf(
				`	
				select
					sku_id,
					ready_stock_count,
					ready_stock_amount,
					now() as last_updated
				from %s
				`,
				s.DependTable(sk, &SkuReadyStock{}),
			),
			OnConflict: []string{"sku_id"},
			Fields:     []string{"ready_stock_count", "ready_stock_amount", "last_updated"},
		}),
		s.UpsertOps(&streaming_compute.UpsertPayload{
			DestinationTable: s.TableName(sk),
			Query: fmt.Sprintf(
				`	
				select
					sku_id,
					ongoing_tx_count,
					ongoing_item_count,
					ongoing_item_amount,
					now() as last_updated
				from %s
				`,
				s.DependTable(sk, &SkuOngoingStock{}),
			),
			OnConflict: []string{"sku_id"},
			Fields:     []string{"ongoing_tx_count", "ongoing_item_count", "ongoing_item_amount", "last_updated"},
		}),
	}
}

// StreamTableName implements [streaming_compute.StreamingTable].
func (s *SkuStock) StreamTableName() string {
	return "sku_stock"
}

// Temporary implements [streaming_compute.StreamingTable].
func (s *SkuStock) Temporary() bool {
	return false
}

// // BuildQuery implements [batch_compute.Table].
// func (s SkuStock) BuildQuery(graph *batch_compute.GraphContext) string {
// 	panic("unimplemented")
// }

// func (s SkuStock) BuildQueries(graph *batch_compute.GraphContext) []string {
// 	insert := streaming_compute.Upsert(
// 		graph.Schema,
// 		s.TableName(),
// 		fmt.Sprintf(
// 			`
// 			select
// 				sku_id,
// 				ready_stock_count,
// 				ready_stock_amount,
// 				now() as last_updated
// 			from %s
// 			`,
// 			graph.DependName(s, SkuReadyStock{}),
// 		),
// 		[]string{"sku_id"},
// 		[]string{"ready_stock_count", "ready_stock_amount", "last_updated"},
// 	)

// 	ongoingInsert := streaming_compute.Upsert(
// 		graph.Schema,
// 		s.TableName(),
// 		fmt.Sprintf(
// 			`
// 			select
// 				sku_id,
// 				ongoing_tx_count,
// 				ongoing_item_count,
// 				ongoing_item_amount,
// 				now() as last_updated
// 			from %s
// 			`,
// 			graph.DependName(s, SkuOngoingStock{}),
// 		),
// 		[]string{"sku_id"},
// 		[]string{"ongoing_tx_count", "ongoing_item_count", "ongoing_item_amount", "last_updated"},
// 	)

// 	return []string{
// 		insert,
// 		ongoingInsert,
// 	}
// }

// // Temporary implements [batch_compute.Table].
// func (s SkuStock) Temporary() bool {
// 	return true
// }

// func (SkuStock) TableName() string {
// 	return "sku_stocks"
// }

type VariantStock struct {
	VariantID         uint64 `gorm:"primarykey"`
	ReadyStockCount   int64
	ReadyStockAmount  float64
	OngoingTxCount    int32
	OngoingItemCount  int64
	OngoingItemAmount float64
	LastUpdated       time.Time
}

// BuildQuery implements [batch_compute.Table].
func (v VariantStock) BuildQuery(graph *batch_compute.GraphContext) string {
	panic("unimplemented")
}

func (s VariantStock) BuildQueries(graph *batch_compute.GraphContext) []string {
	// insert := streaming_compute.Upsert(
	// 	graph.Schema,
	// 	s.TableName(),
	// 	fmt.Sprintf(
	// 		`
	// 		select
	// 			variant_id,
	// 			ready_stock_count,
	// 			ready_stock_amount,
	// 			now() as last_updated
	// 		from %s
	// 		`,
	// 		"",
	// 		// graph.DependName(s, SkuReadyStock{}),
	// 	),
	// 	[]string{"variant_id"},
	// 	[]string{"ready_stock_count", "ready_stock_amount", "last_updated"},
	// )

	// ongoingInsert := streaming_compute.Upsert(
	// 	s.TableName(),
	// 	fmt.Sprintf(
	// 		`
	// 		select
	// 			sku_id,
	// 			ongoing_tx_count,
	// 			ongoing_item_count,
	// 			ongoing_item_amount,
	// 			now() as last_updated
	// 		from %s
	// 		`,
	// 		graph.DependName(s, SkuOngoingStock{}),
	// 	),
	// 	[]string{"sku_id"},
	// 	[]string{"ongoing_tx_count", "ongoing_item_count", "ongoing_item_amount", "last_updated"},
	// )

	return []string{
		// insert,
		// ongoingInsert,
	}
}

// TableName implements [batch_compute.Table].
func (v VariantStock) TableName() string {
	return "variant_stocks"
}

// Temporary implements [batch_compute.Table].
func (v VariantStock) Temporary() bool {
	return false
}

type ProductStock struct {
	ProductID         uint64
	ReadyStockCount   int64
	ReadyStockAmount  float64
	OngoingTxCount    int32
	OngoingItemCount  int64
	OngoingItemAmount float64
	LastUpdated       time.Time
}
