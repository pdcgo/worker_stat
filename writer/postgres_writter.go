package writer

import (
	"log/slog"

	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
	"gorm.io/gorm"
)

type ItemWriter interface {
	Any() any
	GetKey() string
}

func NewPostgresWriter(db *gorm.DB) (stream_utils.ChainNextHandler[ItemWriter], func() error) {

	updateMap := map[int64]any{}
	keyCount := 0

	return func(next stream_utils.ChainNextFunc[ItemWriter]) stream_utils.ChainNextFunc[ItemWriter] {
			return func(item ItemWriter) error {
				var err error

				if updateMap[stream_core.HashKeyString(item.GetKey())] == nil {
					keyCount++
					updateMap[stream_core.HashKeyString(item.GetKey())] = item.Any()
				}

				if keyCount >= 1000 {
					slog.Info("update change to postgres.", slog.Int("key_count", keyCount))
					err = db.Transaction(func(tx *gorm.DB) error {
						for _, item := range updateMap {
							err = tx.Save(item).Error
							if err != nil {
								return err
							}
						}

						return nil
					})

					if err != nil {
						slog.Error(err.Error())
						return nil
					}
					slog.Info("update change to postgres completed.")
					updateMap = map[int64]any{}
					keyCount = 0
				}

				return next(item)
			}
		},
		func() error {
			var err error
			err = db.Transaction(func(tx *gorm.DB) error {
				for _, item := range updateMap {
					err = tx.Save(item).Error
					if err != nil {
						return err
					}
				}

				return nil
			})

			if err != nil {
				slog.Error(err.Error())
				return nil
			}
			return err
		}
}
