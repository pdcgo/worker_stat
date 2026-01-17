package writer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/wargasipil/stream_engine/stream_utils"
	"gorm.io/gorm"
)

type ItemWriter interface {
	Any() any
	GetKey() string
}

func NewPostgresWriter(ctx context.Context, db *gorm.DB, timeoutWriter time.Duration) (stream_utils.ChainNextHandler[ItemWriter], func() error) {

	updateMap := map[string]ItemWriter{}
	keyCount := 0

	timeout := time.NewTimer(timeoutWriter)
	update := make(chan int, 1)
	var lock sync.Mutex

	updateFunc := func() error {
		lock.Lock()
		defer lock.Unlock()

		var err error

		if len(updateMap) == 0 {
			return nil
		}

		slog.Info("update change to postgres.", slog.Int("key_count", keyCount))

		err = db.Transaction(func(tx *gorm.DB) error {
			for _, item := range updateMap {
				err = tx.Save(item.Any()).Error
				if err != nil {
					return err
				}
			}

			return nil
		})

		updateMap = map[string]ItemWriter{}
		keyCount = 0
		slog.Info("update change to postgres completed.")
		return err

	}

	go func() {
		var err error
		for {
			select {
			case <-ctx.Done():
				return
			case <-timeout.C:
				err = updateFunc()
				timeout.Reset(timeoutWriter)

			case <-update:
				err = updateFunc()
				timeout.Reset(timeoutWriter)
			}

			if err != nil {
				slog.Error("updating postgres error", slog.String("error", err.Error()))
			}
		}
	}()

	return func(next stream_utils.ChainNextFunc[ItemWriter]) stream_utils.ChainNextFunc[ItemWriter] {
			return func(item ItemWriter) error {

				if updateMap[item.GetKey()] == nil {
					lock.Lock()
					keyCount++
					updateMap[item.GetKey()] = item
					lock.Unlock()
				}

				if keyCount >= 1000 {
					update <- 1
				}

				return next(item)
			}
		},
		updateFunc
}
