package streaming_compute

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/pdcgo/worker_stat/batch_compute"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

type StreamCompute struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	db   *gorm.DB
	lock sync.Mutex

	computeTables []batch_compute.Table
	schema        string
}

func NewStreamCompute(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	db *gorm.DB,
	schema string,
	computeTables []batch_compute.Table,
) *StreamCompute {
	return &StreamCompute{ctx, cancel, db, sync.Mutex{}, computeTables, schema}
}

func (s *StreamCompute) Process(ctx context.Context, ti *time.Timer, d time.Duration) {
	var err error

	tracer := otel.GetTracerProvider().Tracer("")
	ctx, span := tracer.Start(ctx, "update_stock")
	defer span.End()

	s.lock.Lock()
	defer s.lock.Unlock()
	defer ti.Reset(d)

	graph := batch_compute.NewGraphContext(s.schema, true, &batch_compute.GlobalFilter{})

	err = s.db.Transaction(func(tx *gorm.DB) error {
		slog.Info("processing data..")

		return graph.Compute(
			s.ctx,
			tx,
			s.computeTables...,
		// SkuReadyStockTemp{},
		)

	}, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})

	if err != nil {
		span.RecordError(
			err,
			trace.WithStackTrace(true),
		)
		span.SetStatus(codes.Error, err.Error())

		// s.cancel(err)
		return
	}

	var table SourceTable
	var ok bool
	for _, item := range graph.DependTables() {
		// log.Println(item.TableName(), ok, "asdasdasdasdasd")
		table, ok = item.(SourceTable)
		if !ok {
			continue
		}

		err = table.AfterCalculate(s.db)
		if err != nil {
			span.RecordError(
				err,
				trace.WithStackTrace(true),
			)
			span.SetStatus(codes.Error, err.Error())
		}
	}

	// // deleting change
	// err = s.
	// 	db.
	// 	Session(&gorm.Session{AllowGlobalUpdate: true}).
	// 	Table("test.inv_transaction_changes").
	// 	Delete(&streaming_metric.InvTransactionChange{}).
	// 	Error

	// if err != nil {
	// 	span.SetStatus(codes.Error, err.Error())
	// 	slog.Error("error deleting", "err", err.Error())
	// }

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
	err = graph.GenerateVisualization(f, s.computeTables...)
	if err != nil {
		return err
	}

	return nil
}
