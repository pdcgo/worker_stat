package replication

import (
	"context"
	"sync"

	"cloud.google.com/go/firestore"
	"github.com/jackc/pglogrepl"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ReplicationState interface {
	GetLsn(ctx context.Context) (pglogrepl.LSN, error)
	SetLsn(ctx context.Context, lsn pglogrepl.LSN) error
	SyncLsn(ctx context.Context) error
}

type ReplicationStateData struct {
	Lsn string `json:"lsn"`
}

type memoryReplicationState struct {
	LSN pglogrepl.LSN `json:"lsn"`
}

// SyncLsn implements ReplicationState.
func (m *memoryReplicationState) SyncLsn(ctx context.Context) error {
	return nil
}

func NewMemoryReplicationState() ReplicationState {
	return &memoryReplicationState{
		LSN: pglogrepl.LSN(0),
	}
}

// GetLsn implements ReplicationState.
func (m *memoryReplicationState) GetLsn(ctx context.Context) (pglogrepl.LSN, error) {
	return m.LSN, nil
}

// SetLsn implements ReplicationState.
func (m *memoryReplicationState) SetLsn(ctx context.Context, lsn pglogrepl.LSN) error {
	m.LSN = lsn
	return nil
}

var repCollectionName = "replication_state"

type firestoreReplicationState struct {
	lock    sync.Mutex
	version string
	client  *firestore.Client
	state   ReplicationStateData
}

// SyncLsn implements ReplicationState.
func (f *firestoreReplicationState) SyncLsn(ctx context.Context) error {
	panic("unimplemented")
}

func NewFirestoreReplicationState(ctx context.Context, client *firestore.Client, version string) (ReplicationState, error) {
	lsn := pglogrepl.LSN(0)
	ps := lsn.String()
	repstate := &firestoreReplicationState{sync.Mutex{}, version, client, ReplicationStateData{Lsn: ps}}
	_, err := repstate.GetLsn(ctx)
	return repstate, err
}

// GetLsn implements ReplicationState.
func (f *firestoreReplicationState) GetLsn(ctx context.Context) (pglogrepl.LSN, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	doc := f.client.Collection(repCollectionName).Doc(f.version)

	snap, err := doc.Get(ctx)
	if err != nil {

		e, ok := status.FromError(err)
		if ok {
			if e.Code() == codes.NotFound {
				f.lock.Unlock()
				err = f.SetLsn(ctx, 0)
				f.lock.Lock()
				if err != nil {
					return 0, err
				}
				return pglogrepl.LSN(0), nil

			} else {
				return 0, err
			}

		} else {
			return 0, err
		}

	}

	var data ReplicationStateData
	err = snap.DataTo(&data)
	if err != nil {
		return 0, err
	}

	return pglogrepl.ParseLSN(f.state.Lsn)
}

// SetLsn implements ReplicationState.
func (f *firestoreReplicationState) SetLsn(ctx context.Context, lsn pglogrepl.LSN) error {
	var err error
	f.lock.Lock()
	defer f.lock.Unlock()

	f.state.Lsn = lsn.String()

	doc := f.client.Collection(repCollectionName).Doc(f.version)
	_, err = doc.Set(ctx, &f.state)
	return err

}
