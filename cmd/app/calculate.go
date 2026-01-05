package main

import (
	"context"
	"log"
	"time"

	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/urfave/cli/v3"
	"github.com/wargasipil/stream_engine/stream_core"
	"gorm.io/gorm"
)

type CalculateFunc cli.ActionFunc

func NewCalculate(
	cfg *stream_core.CoreConfig,
	db *gorm.DB,
	process ProcessHandler,
	snapshot PeriodicSnapshot,
	kv *stream_core.HashMapCounter,

) CalculateFunc {
	return func(ctx context.Context, c *cli.Command) error {
		var err error
		err = kv.ResetCounter()
		if err != nil {
			log.Fatal(err)
		}

		// duration := time.Minute
		// tick := time.NewTimer(duration)

		// last := time.Now()
		// go func() {
		// 	for {
		// 		select {
		// 		case <-tick.C:
		// 			err = snapshot(last)
		// 			if err != nil {
		// 				slog.Error(err.Error())
		// 			}
		// 			tick.Reset(duration)
		// 			last = time.Now()
		// 		case <-context.Background().Done():
		// 			return
		// 		}
		// 	}
		// }()

		// iterating journal entries
		var entries []*accounting_core.JournalEntry
		for {

			pkey := kv.GetUint64("accounting_pkey")
			entries = []*accounting_core.JournalEntry{}
			err = db.
				Model(&accounting_core.JournalEntry{}).
				Joins("join transactions on journal_entries.transaction_id = transactions.id").
				Preload("Account").
				Where("transactions.created > ?", "2026-01-05").
				Where("journal_entries.id > ?", pkey).
				Limit(5000).
				// Limit(5).
				Order("id asc").
				Find(&entries).
				Error

			if err != nil {
				log.Fatal(err)
			}

			if len(entries) == 0 {
				time.Sleep(time.Second * 30)
			}

			for _, entry := range entries {
				err = process(entry)
				if err != nil {
					log.Fatal(err)
				}
				kv.PutUint64("accounting_pkey", uint64(entry.ID))
			}

			// test
			// break
		}

		return nil
	}
}
