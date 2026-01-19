package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pdcgo/shared/db_models"
	"github.com/urfave/cli/v3"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_storage"
	"github.com/wargasipil/stream_engine/stream_utils"
	"go.mongodb.org/mongo-driver/bson"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const LAST_PRIMARY_KEYNAME = "last_inventory_log_id"

type RunStreamFunc cli.ActionFunc

func NewRunStream(
	db *gorm.DB,
	streamConfig *ConfigStream,
	wal *stream_storage.WalStream,
	kv stream_core.KeyStore,

) RunStreamFunc {

	signals := make(chan os.Signal, 1)

	// Register the channel to receive SIGINT (Ctrl+C) and SIGTERM signals.
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	lastStateHandler := func(next stream_utils.ChainNextFunc[*BatchInventoryLog]) stream_utils.ChainNextFunc[*BatchInventoryLog] {
		return func(data *BatchInventoryLog) error {
			lastID := data.Entries[len(data.Entries)-1].ID
			kv.PutInt64(LAST_PRIMARY_KEYNAME, int64(lastID))
			return next(data)
		}
	}

	backfillHandler := stream_utils.NewChain(
		func(next stream_utils.ChainNextFunc[*BatchInventoryLog]) stream_utils.ChainNextFunc[*BatchInventoryLog] {
			return func(data *BatchInventoryLog) error {
				err := wal.Replay(func(data []byte) error {
					var batch BatchInventoryLog
					err := bson.Unmarshal(data, &batch)
					if err != nil {
						return err
					}

					return next(&batch)
				})

				if err != nil {
					return err
				}
				return nil
			}
		},
		// updating last state
		lastStateHandler,
	)

	liveHandler := stream_utils.NewChain(
		// produce event
		func(next stream_utils.ChainNextFunc[*BatchInventoryLog]) stream_utils.ChainNextFunc[*BatchInventoryLog] {
			return func(data *BatchInventoryLog) error {
			Loop:
				for {
					select {
					case <-signals:
						err := wal.Close()
						if err != nil {
							panic(err)
						}
						slog.Info("receiving signal")
						break Loop
					default:
						var logs []*InventoryLog
						err := db.
							Model(&InventoryLog{}).
							Limit(4000).
							Order("id asc").
							Where("id > ?", kv.GetUint64(LAST_PRIMARY_KEYNAME)).
							Find(&logs).
							Error

						if err != nil {
							return err
						}

						if len(logs) == 0 {
							wal.Flush()
							// time.Sleep(time.Minute)
							continue
						}
						slog.Info("processing batch event", slog.Uint64("last_pkey", kv.GetUint64(LAST_PRIMARY_KEYNAME)))
						batch := BatchInventoryLog{
							Entries: logs,
						}
						err = next(&batch)
						if err != nil {
							return err
						}
					}

				}

				return nil
			}
		},
		// writing to wal
		func(next stream_utils.ChainNextFunc[*BatchInventoryLog]) stream_utils.ChainNextFunc[*BatchInventoryLog] {
			return func(data *BatchInventoryLog) error {
				slog.Info("writing to wal..")
				rawdata, err := bson.Marshal(data)
				if err != nil {
					return err
				}
				err = wal.Append(rawdata)
				if err != nil {
					return err
				}

				return next(data)
			}
		},
		// updating last processed state
		lastStateHandler,
	)

	return func(ctx context.Context, c *cli.Command) error {
		var err error
		defer kv.Close()
		defer wal.Close()

		// removing counter

		// replay wal
		slog.Info("replay wal on storage...")
		err = backfillHandler(&BatchInventoryLog{})
		slog.Info("replay wal in storage completed..")

		// running live counter
		slog.Info("running live counter...")
		err = liveHandler(&BatchInventoryLog{})
		if err != nil {
			return err
		}

		return nil
	}

}

type BatchInventoryLog struct {
	Entries []*InventoryLog
}

type Action string

const (
	ActionInsert Action = "INSERT"
	ActionDelete Action = "DELETE"
	ActionUpdate Action = "UPDATE"
)

type InventoryLog struct {
	ID       uint `gorm:"primarykey"`
	Action   Action
	OldData  datatypes.JSONType[*db_models.InvertoryHistory]
	NewData  datatypes.JSONType[*db_models.InvertoryHistory]
	ChangeAt time.Time
}

func (InventoryLog) TableName() string {
	return "inventory_log"
}
