package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/pdcgo/shared/configs"
	"github.com/pdcgo/shared/db_connect"
	"github.com/wargasipil/stream_engine/stream_core"
	"gorm.io/gorm"
)

func NewStreamCoreConfig() *stream_core.CoreConfig {
	return &stream_core.CoreConfig{
		HashMapCounterPath:  "/tmp/worker_stat/counter",
		HashMapCounterSlots: 134_217_728,
		DynamicValuePath:    "/tmp/worker_stat/value",
	}
}

func NewDatabase(cfg *configs.AppConfig) (*gorm.DB, error) {
	return db_connect.NewProductionDatabase("worker-stat", &cfg.Database)
}

type Worker struct {
	Run func() error
}

func NewWorker(
	cfg *stream_core.CoreConfig,
	db *gorm.DB,
	process ProcessHandler,
	snapshot PeriodicSnapshot,
	kv *stream_core.HashMapCounter,
) *Worker {
	return &Worker{
		Run: func() error {

			var err error

			defer kv.Close()
			err = kv.ResetCounter()
			if err != nil {
				log.Fatal(err)
			}

			duration := time.Minute
			tick := time.NewTimer(duration)

			last := time.Now()
			go func() {
				for {
					select {
					case <-tick.C:
						err = snapshot(last)
						if err != nil {
							slog.Error(err.Error())
						}
						tick.Reset(duration)
						last = time.Now()
					case <-context.Background().Done():
						return
					}
				}
			}()

			defer snapshot(time.Now().AddDate(-1, 0, 0))

			// iterating journal entries
			var entries []*accounting_core.JournalEntry
			for {

				pkey := kv.GetUint64("accounting_pkey")
				entries = []*accounting_core.JournalEntry{}
				err = db.
					Model(&accounting_core.JournalEntry{}).
					Preload("Account").
					Where("entry_time > ?", "2025-12-28").
					Where("id > ?", pkey).
					Limit(5000).
					Order("id asc").
					Find(&entries).
					Error

				if err != nil {
					log.Fatal(err)
				}

				if len(entries) == 0 {
					break
				}

				for _, entry := range entries {
					err = process(entry)
					if err != nil {
						log.Fatal(err)
					}
					kv.PutUint64("accounting_pkey", uint64(entry.ID))
				}

			}

			return nil
		},
	}
}

func main() {

	os.MkdirAll("/tmp/worker_stat", os.ModeDir)

	worker, err := InitializeWorker()
	if err != nil {
		log.Fatal(err)
	}

	err = worker.Run()
	if err != nil {
		log.Fatal(err)
	}

}
