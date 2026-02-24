package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/pdcgo/shared/configs"
	"github.com/pdcgo/shared/db_models"
	"github.com/pdcgo/shared/pkg/common_helper"
	"github.com/pdcgo/shared/pkg/debugtool"
	"github.com/pdcgo/worker_stat/batch_compute"
	"github.com/pdcgo/worker_stat/replication"
	"github.com/urfave/cli/v3"
	"gorm.io/gorm"
)

type StreamingFunc cli.ActionFunc

func NewStreaming(
	cfg *configs.AppConfig,
	db *gorm.DB,
) StreamingFunc {
	return func(ctx context.Context, c *cli.Command) error {
		var err error

		// migration first
		err = db.AutoMigrate(
			&InvTransactionChange{},
		)
		if err != nil {
			return err
		}

		replicate, err := replication.ConnectReplication(ctx, &cfg.Database)

		if err != nil {
			return err
		}

		ctx, cancel := context.WithCancelCause(ctx)

		compute := NewStreamCompute(ctx, cancel, db)
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

					change := InvTransactionChange{
						At:      time.Now().UnixMicro(),
						TxID:    uint64(id),
						ModType: data.ModType,
						TxType:  db_models.InvTxType(tx_type),
					}

					debugtool.LogJson(change)

					err = db.Save(&change).Error
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

func (s *StreamCompute) Process(ti *time.Timer, d time.Duration) {
	var err error

	s.lock.Lock()
	defer s.lock.Unlock()
	defer ti.Reset(d)

	err = s.db.Transaction(func(tx *gorm.DB) error {
		slog.Info("processing data..")

		graph := batch_compute.NewGraphContext("test", true, &batch_compute.GlobalFilter{})
		return graph.Compute(
			s.ctx,
			tx,
			SkuReadyStock{},
		)

	}, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})

	// deleting change
	err = s.
		db.
		Session(&gorm.Session{AllowGlobalUpdate: true}).
		Delete(&InvTransactionChange{}).
		Error

	if err != nil {
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
			s.Process(ti, d)
		}
	}
}

func (s *StreamCompute) Lock() {
	s.lock.Lock()
}

func (s *StreamCompute) Unlock() {
	s.lock.Unlock()
}

type InvTransactionChange struct {
	At      int64 `gorm:"primarykey"`
	TxID    uint64
	TxType  db_models.InvTxType
	ModType replication.ModificationType
}

type SkuReadyStock struct{}

// BuildQuery implements [batch_compute.Table].
func (s SkuReadyStock) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
		with skus as (
			select 
				distinct iti.sku_id as sku_id
			from public.inv_transaction_changes itc 
			left join public.inv_tx_items iti on iti.inv_transaction_id = itc.tx_id
			where itc.mod_type = 'insert'
		)

		select
			ih.sku_id as sku_id,
			sum(ih.count * -1) as ready_stock
		from skus s
		left join public.invertory_histories ih on ih.sku_id = s.sku_id
		where
			ih.tx_id is null
		group by (
			ih.sku_id
		)
	`
}

// TableName implements [batch_compute.Table].
func (s SkuReadyStock) TableName() string {
	return "sku_ready_stock"
}

// Temporary implements [batch_compute.Table].
func (s SkuReadyStock) Temporary() bool {
	return false
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
