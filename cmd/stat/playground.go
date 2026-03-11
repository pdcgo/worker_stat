package main

import (
	"context"
	"database/sql"
	"log/slog"

	"github.com/pdcgo/worker_stat/streaming_compute"
	"github.com/urfave/cli/v3"
	"gorm.io/gorm"
)

type PlaygroundFunc cli.ActionFunc

func NewPlayground(
	db *gorm.DB,
) PlaygroundFunc {
	return func(ctx context.Context, c *cli.Command) error {
		slog.SetLogLoggerLevel(slog.LevelDebug)

		var err error
		var schema string = "test"

		stream := streaming_compute.NewStreamingContext(
			streaming_compute.WithSchemaOption(schema),
			streaming_compute.WithDisableTemporary(),
		)

		// registering source
		err = stream.RegisterSource(db,
			&MockSource{},
		)
		if err != nil {
			return err
		}

		caller := stream.Compute(
			StockCount{},
		)

		err = stream.EmitToSource(db, &MockSource{
			Data: 5,
		})
		if err != nil {
			return err
		}

		err = db.Transaction(func(tx *gorm.DB) error {
			err = caller(ctx, tx)

			if err != nil {
				return err
			}

			err = stream.EmitToSource(tx, StockCount{
				UserId: 12333333,
			})

			return err

		}, &sql.TxOptions{
			Isolation: sql.LevelRepeatableRead,
		})

		return err
	}
}

type MockSource struct {
	Data int64
}

// Temporary implements [streaming_compute.StreamingSource].
func (m *MockSource) Temporary() bool {
	return false
}

// IsSource implements [streaming_compute.StreamingSource].
func (m *MockSource) IsSource() bool {
	return true
}

// StreamTableName implements [streaming_compute.StreamingSource].
func (m *MockSource) StreamTableName() string {
	return "mock_source"
}

type StockCount struct {
	UserId uint64
}

// IsSource implements [streaming_compute.StreamingSource].
func (stock StockCount) IsSource() bool {
	return true
}

// BuildQueries implements [streaming_compute.StreamingTable].
func (stock StockCount) BuildQueries(s *streaming_compute.StreamingContext) []string {
	return []string{
		s.InsertOps(stock,
			`select id from users limit 10`,
		),
	}
}

// StreamTableName implements [streaming_compute.StreamingTable].
func (s StockCount) StreamTableName() string {
	return "stock_count"
}

// Temporary implements [streaming_compute.StreamingTable].
func (s StockCount) Temporary() bool {
	return true
}
