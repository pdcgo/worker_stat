package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/pdcgo/shared/configs"
	"github.com/pdcgo/shared/custom_connect"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/pkg/common_helper"
	"github.com/pdcgo/shared/pkg/debugtool"
	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/pdcgo/worker_stat/replication"
	"github.com/pdcgo/worker_stat/streaming_compute"
	"github.com/pdcgo/worker_stat/streaming_metric"
	"github.com/urfave/cli/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"gorm.io/gorm"
)

type StreamingFunc cli.ActionFunc

func NewStreaming(
	cfg *configs.AppConfig,
	db *gorm.DB,
) StreamingFunc {
	return func(ctx context.Context, c *cli.Command) error {
		var err error

		// initialize trace
		cancelTrace, err := custom_connect.InitTracer("stock_updater")
		if err != nil {
			return err
		}

		defer cancelTrace(ctx)

		// migration first
		err = db.AutoMigrate(
			streaming_metric.SkuStock{},
		)
		if err != nil {
			return err
		}

		// create source data
		saveInvTransactionChange, err := streaming_compute.NewSource(db, "test", &streaming_metric.InvTransactionChange{})
		if err != nil {
			return err
		}

		// create replication context
		replicate, err := replication.ConnectReplication(ctx, &cfg.Database)

		if err != nil {
			return err
		}

		ctx, cancel := context.WithCancelCause(ctx)

		compute := NewStreamCompute(ctx, cancel, db)

		// generating visualization
		visual := c.String("visualization")
		if visual != "" {
			err = compute.GenerateVisualization(visual)
			if err != nil {
				return err
			}
		}

		go compute.Compute(time.Second * 15)

		process := common_helper.NewChainParam(
			func(next common_helper.NextFuncParam[*replication.ReplicationEvent]) common_helper.NextFuncParam[*replication.ReplicationEvent] {
				return func(event *replication.ReplicationEvent) error { // filtering cuma inv_transaction
					switch event.SourceMetadata.Table {
					case "inv_transactions":
						// shared locking dengan compute
						compute.Lock()
						defer compute.Unlock()

						return next(event)
					default:
						return nil
					}
				}
			},
			func(next common_helper.NextFuncParam[*replication.ReplicationEvent]) common_helper.NextFuncParam[*replication.ReplicationEvent] {
				return func(data *replication.ReplicationEvent) error {
					// log.Println(data.SourceMetadata.Table, data.ModType)

					id, ok := data.Data["id"].(int64)
					if !ok {
						return errors.New("cannot get id")
					}

					tx_type := data.Data["type"].(string)
					status := data.Data["type"].(string)

					change := streaming_metric.InvTransactionChange{
						At:      time.Now().UnixMicro(),
						TxID:    uint64(id),
						ModType: data.ModType,
						TxType:  db_models.InvTxType(tx_type),
						Status:  db_models.InvTxStatus(status),
					}

					debugtool.LogJson(change)
					err = saveInvTransactionChange(&change)
					return err
				}
			},
		)

		err = replicate.StreamStart(ctx, "test", "stat_publication", func(ctx context.Context, event *replication.ReplicationEvent) error {
			return process(event)
		})

		return err
	}
}

type StreamCompute struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	db   *gorm.DB
	lock sync.Mutex
}

func NewStreamCompute(ctx context.Context, cancel context.CancelCauseFunc, db *gorm.DB) *StreamCompute {
	return &StreamCompute{ctx, cancel, db, sync.Mutex{}}
}

func (s *StreamCompute) createTableCompute() []batch_compute.Table {
	result := []batch_compute.Table{
		streaming_metric.SkuStock{},
	}

	return result
}

func (s *StreamCompute) Process(ctx context.Context, ti *time.Timer, d time.Duration) {
	var err error

	trace := otel.GetTracerProvider().Tracer("")
	ctx, span := trace.Start(ctx, "update_stock")
	defer span.End()

	s.lock.Lock()
	defer s.lock.Unlock()
	defer ti.Reset(d)

	err = s.db.Transaction(func(tx *gorm.DB) error {
		slog.Info("processing data..")

		graph := batch_compute.NewGraphContext("test", true, &batch_compute.GlobalFilter{})
		tableToCompute := s.createTableCompute()
		return graph.Compute(
			s.ctx,
			tx,
			tableToCompute...,
		// SkuReadyStockTemp{},
		)

	}, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})

	// deleting change
	err = s.
		db.
		Session(&gorm.Session{AllowGlobalUpdate: true}).
		Table("test.inv_transaction_changes").
		Delete(&streaming_metric.InvTransactionChange{}).
		Error

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		slog.Error("error deleting", "err", err.Error())
	}

}

func (s *StreamCompute) Compute(d time.Duration) {

	ti := time.NewTimer(d)
	defer ti.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ti.C:
			ctx, cancel := context.WithTimeout(s.ctx, time.Minute*15)
			s.Process(ctx, ti, d)
			cancel()
		}
	}
}

func (s *StreamCompute) Lock() {
	s.lock.Lock()
}

func (s *StreamCompute) Unlock() {
	s.lock.Unlock()
}

func (s *StreamCompute) GenerateVisualization(fname string) error {
	slog.Info("generate visualization", "path", fname)
	graph := batch_compute.NewGraphContext("dump", false, &batch_compute.GlobalFilter{})
	f, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	err = graph.GenerateVisualization(f, s.createTableCompute()...)
	if err != nil {
		return err
	}

	return nil
}

type SkuOngoingStock struct{}

// BuildQuery implements [batch_compute.Table].
func (s SkuOngoingStock) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
	
	`
}

// TableName implements [batch_compute.Table].
func (s SkuOngoingStock) TableName() string {
	return "sku_ongoing_stock"
}

// Temporary implements [batch_compute.Table].
func (s SkuOngoingStock) Temporary() bool {
	return false
}

type IncrementalTable struct {
	table batch_compute.Table
}

// BuildQuery implements [batch_compute.Table].
func (i *IncrementalTable) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
	
	`
}

// TableName implements [batch_compute.Table].
func (i *IncrementalTable) TableName() string {
	return fmt.Sprintf("%s_inc", i.table.TableName())
}

// Temporary implements [batch_compute.Table].
func (i *IncrementalTable) Temporary() bool {
	return false
}

func (i *IncrementalTable) BuildQueries(graph *batch_compute.GraphContext) []string {
	queries := []string{}
	queries = graph.BuildQueries(i.table)

	tableName := graph.GetTableName(i.table)
	// copy table if not exists
	queries = append(queries,
		fmt.Sprintf(
			"create table if not exists %s.%s as\n %s",
			graph.Schema,
			tableName,
			i.table.BuildQuery(graph),
		),
	)

	return queries

}

func NewIncrementalTable(table batch_compute.Table) batch_compute.Table {
	return &IncrementalTable{table}
}

// var cc batch_compute.Table = SkuOngoingStock{}
