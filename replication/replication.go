package replication

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"regexp"
	"time"

	"github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/proxy"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pdcgo/shared/configs"
)

func ConnectReplication(ctx context.Context, dbconfig configs.DatabaseConfig, state ReplicationState) (*Replication, error) {
	dsn := dbconfig.ToDsn("stat_streaming")
	dsn += " replication=database"

	dbconf, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	dbconf.LookupFunc = func(ctx context.Context, host string) (addrs []string, err error) {
		return []string{host}, nil
	}
	dbconf.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		re := regexp.MustCompile(`\[(.*?)\]`)
		match := re.FindStringSubmatch(addr)
		if len(match) <= 1 {
			return nil, fmt.Errorf("statreplica error dialing %s to %s", network, addr)
		}
		return proxy.Dial(match[1])
	}

	conn, err := pgconn.ConnectConfig(ctx, dbconf)

	if err != nil {
		return nil, err
	}

	parser := NewV2Parser(ctx)
	return &Replication{conn, state, parser}, nil
}

type ReplicationHandler func(ctx context.Context, event *ReplicationEvent) error

type Replication struct {
	conn   *pgconn.PgConn
	state  ReplicationState
	parser Parser
}

func (r *Replication) Close(ctx context.Context) error {
	return r.conn.Close(ctx)
}

func (r *Replication) RepeatableRead(ctx context.Context) (commit func() error, err error) {
	commit = func() error {
		_, err := r.conn.Exec(ctx, "COMMIT").ReadAll()
		return err
	}

	_, err = r.conn.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ").ReadAll()
	if err != nil {
		return commit, err
	}
	return commit, nil
}

func (r *Replication) GetLsn(ctx context.Context) (pglogrepl.LSN, error) {
	result := r.conn.Exec(ctx, "SELECT pg_current_wal_lsn()")
	rows, err := result.ReadAll()
	if err != nil {
		return 0, err
	}

	lsnStr := string(rows[0].Rows[0][0])
	return pglogrepl.ParseLSN(lsnStr)
}

func (r *Replication) StreamStart(
	ctx context.Context,
	slotName string,
	publicationName string,
	handler ReplicationHandler,
) error {
	pctx := ctx
	slotMng := SlotManage{r.conn}
	slotName, err := slotMng.SlotInitialize(ctx, slotName, true)

	if err != nil {
		return err
	}

	pubnames, err := slotMng.PublicationList(ctx)
	if err != nil {
		return err
	}
	slog.Info("publication list", "publication", pubnames)

	slog.Info("start streaming", "slot", slotName)
	start, err := r.state.GetLsn(ctx)
	if err != nil {
		return err
	}

	pluginArguments := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", publicationName),
		"messages 'true'",
		"streaming 'true'",
	}
	err = pglogrepl.StartReplication(ctx, r.conn, slotName, start, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return err
	}

	clientXLogPos := start
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				return err
			}
			log.Printf("Sent Standby status message at %s\n", clientXLogPos.String())
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
		rawMsg, err := r.conn.ReceiveMessage(ctx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}

			return err
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("Received unexpected message: %T\n", rawMsg)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return err
			}
			// log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)
			if pkm.ServerWALEnd > clientXLogPos {
				clientXLogPos = pkm.ServerWALEnd
				err = r.state.SetLsn(pctx, clientXLogPos)
				if err != nil {
					return err
				}
			}

			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return err
			}

			msg, err := r.parser.Parse(xld.WALData)
			if err != nil {
				return err
			}

			if msg != nil {
				err = handler(ctx, msg)
				if err != nil {
					return err
				}
			}

			if xld.WALStart > clientXLogPos {

				clientXLogPos = xld.WALStart
				err = r.state.SetLsn(pctx, clientXLogPos)
				if err != nil {
					return err
				}
			}
		}
	}

	// return err
}
