package replication

import (
	"context"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

type SlotManage struct {
	conn *pgconn.PgConn
}

func (s *SlotManage) SlotList(ctx context.Context) ([]string, error) {
	var result []string
	query := "SELECT * FROM pg_replication_slots"
	res := s.conn.Exec(ctx, query)
	datas, err := res.ReadAll()

	if err != nil {
		return result, err
	}

	for _, data := range datas {
		if data.Err != nil {
			return result, data.Err
		}
		for _, columns := range data.Rows {
			result = append(result, string(columns[0]))
		}
	}
	return result, nil
}

func (s *SlotManage) SlotDrop(ctx context.Context, slotName string) error {
	return pglogrepl.DropReplicationSlot(ctx, s.conn, slotName, pglogrepl.DropReplicationSlotOptions{
		Wait: true,
	})
}

func (s *SlotManage) SlotCreate(ctx context.Context, slotName string, temporary bool) error {
	_, err := pglogrepl.CreateReplicationSlot(ctx, s.conn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: temporary})
	return err
}

func (s *SlotManage) SlotInitialize(ctx context.Context, slotName string, temporary bool) (string, error) {
	// checking and reuse slotlist
	var err error

	var actualSlotName string

	slots, err := s.SlotList(ctx)
	if err != nil {
		return "", err
	}

	for _, slot := range slots {
		if strings.HasPrefix(slot, slotName) {

			actualSlotName = slot
			return actualSlotName, nil
		}
	}

	suffix := uuid.New().String()
	suffix = strings.ReplaceAll(suffix, "-", "_")
	actualSlotName = slotName + "_" + suffix

	slog.Info("slot not found, creating one", "slot", actualSlotName)

	err = s.SlotCreate(ctx, actualSlotName, temporary)
	if err != nil {
		return "", err
	}

	return actualSlotName, nil

}

func (s *SlotManage) PublicationList(ctx context.Context) ([]string, error) {
	var result []string
	query := "SELECT * FROM pg_publication"
	res := s.conn.Exec(ctx, query)
	datas, err := res.ReadAll()

	if err != nil {
		return result, err
	}

	for _, data := range datas {
		if data.Err != nil {
			return result, data.Err
		}
		for _, columns := range data.Rows {
			result = append(result, string(columns[1]))
		}
	}
	return result, nil
}
