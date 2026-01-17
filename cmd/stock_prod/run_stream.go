package main

import (
	"context"
	"log"
	"log/slog"
	"time"

	"github.com/urfave/cli/v3"
	"github.com/wargasipil/stream_engine/stream_storage"
	"go.mongodb.org/mongo-driver/bson"
	"gorm.io/gorm"
)

type RunStreamFunc cli.ActionFunc

func NewRunStream(
	db *gorm.DB,
	streamConfig *ConfigStream,
	wal *stream_storage.WalStream,
) RunStreamFunc {

	backfillHandler

	return func(ctx context.Context, c *cli.Command) error {
		var err error
		// removing counter

		// replay wal
		slog.Info("replay wal on storage...")
		err = wal.Replay(func(data []byte) error {
			var batch BatchInventoryLog
			err := bson.Unmarshal(data, &batch)
			if err != nil {
				return err
			}

			log.Println(string(data))
			return nil
		})

		if err != nil {
			return err
		}
		slog.Info("replay wal in storage completed..")

		// running live counter
		slog.Info("running live counter...")

		var logs []*InventoryLog

		err = db.
			Model(&InventoryLog{}).
			Limit(10).
			Find(&logs).
			Error

		for _, data := range logs {
			log.Println(string(data.NewData))
		}

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
	OldData  []byte
	NewData  []byte
	ChangeAt time.Time
}

func (InventoryLog) TableName() string {
	return "inventory_log"
}
