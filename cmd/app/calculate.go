package main

import (
	"context"
	"log"
	"time"

	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/pdcgo/worker_stat/batch_model"
	"github.com/pdcgo/worker_stat/metric/metric_shop"
	"github.com/pdcgo/worker_stat/writer"
	"github.com/urfave/cli/v3"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_storage"
	"github.com/wargasipil/stream_engine/stream_utils"
	"go.mongodb.org/mongo-driver/bson"
	"gorm.io/gorm"
)

type CalculateFunc cli.ActionFunc

func NewCalculate(
	cfg *stream_core.CoreConfig,
	db *gorm.DB,
	statdb *StatDatabase,
	process ProcessHandler,
	snapshot PeriodicSnapshot,
	kv stream_core.KeyStore,
	wal *stream_storage.WalStream,

) CalculateFunc {

	pgwriterNext, pgwriterClose := writer.NewPostgresWriter(context.Background(), statdb.DB, time.Minute)
	defer pgwriterClose()

	pgwriter := stream_utils.NewChain(pgwriterNext)

	liveHandler := stream_utils.NewChain(
		EnrichShop(db), // enrich data
		WriteWall(wal),
		metric_shop.DailyShopCalculate(kv, pgwriter),
		stream_utils.ChainNextHandler[*batch_model.BatchJournalEntry](process),
	)

	backfillHandler := stream_utils.NewChain(
		metric_shop.DailyShopCalculate(kv, pgwriter),
		stream_utils.ChainNextHandler[*batch_model.BatchJournalEntry](process),
	)

	return func(ctx context.Context, c *cli.Command) error {
		var err error

		defer wal.Close()

		// running backfill first
		err = wal.Replay(func(data []byte) error {
			var batch batch_model.BatchJournalEntry
			err := bson.Unmarshal(data, &batch)
			if err != nil {
				return err
			}

			batch.Backfill = true

			err = backfillHandler(&batch)
			if err != nil {
				return err
			}
			return err
		})

		if err != nil {
			return err
		}

		log.Println("backfill completed..")

		// periodic snapshot
		go snapshot(context.Background(), time.Minute)

		// iterating journal entries
		var entries []*accounting_core.JournalEntry

		for {

			pkey := kv.GetUint64("accounting_pkey")
			entries = []*accounting_core.JournalEntry{}
			err = db.
				Model(&accounting_core.JournalEntry{}).
				Joins("join transactions on journal_entries.transaction_id = transactions.id").
				Preload("Account").
				Preload("Transaction").
				Where("transactions.created > ?", "2026-01-01").
				Where("journal_entries.id > ?", pkey).
				Limit(5000).
				// Limit(5).
				Order("id asc").
				Find(&entries).
				Error

			if err != nil {
				log.Fatal(err)
			}

			// teamIDs := []uint{}
			// for _, entry := range entries {
			// 	teamIDs = append(teamIDs, entry.Transaction.TeamID, entry.Account.TeamID)
			// }

			// slog.Info("calculate daily Balance")
			// err = calculateBalance(teamIDs)
			// if err != nil {
			// 	return err
			// }

			if len(entries) == 0 {
				time.Sleep(time.Minute)
			}

			// for batch and enrich data
			batch := batch_model.BatchJournalEntry{
				Entries: entries,
			}
			err = liveHandler(&batch)
			if err != nil {
				log.Fatal(err)
			}

			// test
			// break
		}

		// return nil
	}
}

func EnrichShop(db *gorm.DB) stream_utils.ChainNextHandler[*batch_model.BatchJournalEntry] {
	return func(next stream_utils.ChainNextFunc[*batch_model.BatchJournalEntry]) stream_utils.ChainNextFunc[*batch_model.BatchJournalEntry] {
		return func(batch *batch_model.BatchJournalEntry) error {
			txshops := []*accounting_core.TransactionShop{}

			txids := []uint{}
			for _, entry := range batch.Entries {
				txids = append(txids, entry.Transaction.ID)
			}

			err := db.
				Model(&accounting_core.TransactionShop{}).
				Where("transaction_id in ?", txids).
				Find(&txshops).
				Error

			if err != nil {
				return err
			}

			batch.Shop = map[uint]*accounting_core.TransactionShop{}
			for _, txshop := range txshops {
				batch.Shop[txshop.TransactionID] = txshop
			}

			return next(batch)
		}
	}
}

func WriteWall(wal *stream_storage.WalStream) stream_utils.ChainNextHandler[*batch_model.BatchJournalEntry] {
	return func(next stream_utils.ChainNextFunc[*batch_model.BatchJournalEntry]) stream_utils.ChainNextFunc[*batch_model.BatchJournalEntry] {
		return func(batch *batch_model.BatchJournalEntry) error {
			if batch.Backfill {
				return next(batch)
			}

			data, err := bson.Marshal(batch)
			if err != nil {
				return err
			}

			err = wal.Append(data)
			if err != nil {
				return err
			}

			return next(batch)
		}
	}
}
