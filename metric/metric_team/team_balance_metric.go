package metric_team

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/wargasipil/stream_engine/stream_core"
)

// jangan DIEDIT, file ini generate an dari package github.com/wargasipil/stream_engine

type MetricTeamLastBalance struct {
	key string
	Name string
	store stream_core.KeyStore

	// Jangan Diubah letterlek
	TeamID uint64
}

func NewMetricTeamLastBalance(store stream_core.KeyStore, TeamID uint64) *MetricTeamLastBalance {
	keys := []string{}
	names := []string{}
	keys = append(keys, fmt.Sprintf("%d", TeamID))
	names = append(names, "team")
	key := fmt.Sprintf("%s/%s", strings.Join(names, "_"), strings.Join(keys, "/"))
	Name := strings.Join(names, "_")

	return &MetricTeamLastBalance{
		store: store,
		Name: Name,
		key: key,
		TeamID: TeamID,
	}
}

func NewMetricTeamLastBalanceFromKey(store stream_core.KeyStore, mkey string) (*MetricTeamLastBalance, error) {

	var err error

	keys := strings.Split(mkey, "/")
	if len(keys) <= 2 {
		return nil, errors.New("key invalid")
	}
	Name := keys[0]
	names := strings.Split(Name, "_")
	indexkeys := keys[1:]
	key := Name + "/" + strings.Join(indexkeys[:len(names)], "/")
	if len(indexkeys) <= 1 {
		return nil, errors.New("index on key invalid")
	}
	var TeamID uint64
	TeamID, err = strconv.ParseUint(indexkeys[0], 10, 64)

	if err != nil {
		return nil, err
	}

	return &MetricTeamLastBalance{
		store: store,
		Name: Name,
		key: key,
		TeamID: TeamID,
	}, err
}

func IsMetricTeamLastBalance(key string) bool {
	return strings.HasPrefix(key, "team/")
}

func (m *MetricTeamLastBalance) PutTs(value int64) int64 {
	return m.store.PutInt64(m.key + "/ts", value)
}

func (m *MetricTeamLastBalance) IncTs(value int64) int64 {
	return m.store.IncInt64(m.key + "/ts", value)
}

func (m *MetricTeamLastBalance) GetTs() int64 {
	return m.store.GetInt64(m.key + "/ts")
}

func (m *MetricTeamLastBalance) PutBalance(value float64) float64 {
	return m.store.PutFloat64(m.key + "/balance", value)
}

func (m *MetricTeamLastBalance) IncBalance(value float64) float64 {
	return m.store.IncFloat64(m.key + "/balance", value)
}

func (m *MetricTeamLastBalance) GetBalance() float64 {
	return m.store.GetFloat64(m.key + "/balance")
}

func (m *MetricTeamLastBalance) GetKey() string {
	return m.key
}

func (m *MetricTeamLastBalance) Values() map[string]any {
	return map[string]any{
		"ID": stream_core.HashKeyString(m.key),
		"TeamID": m.TeamID,
		"Ts": m.GetTs(),
		"Balance": m.GetBalance(),
	}
}

func (m *MetricTeamLastBalance) Data() *TeamLastBalance {
	return &TeamLastBalance{
		ID: stream_core.HashKeyString(m.key),
		TeamID: m.TeamID,
		Ts: m.GetTs(),
		Balance: m.GetBalance(),
	}
}

func (m *MetricTeamLastBalance) Any() any {
	return &TeamLastBalance{
		ID: stream_core.HashKeyString(m.key),
		TeamID: m.TeamID,
		Ts: m.GetTs(),
		Balance: m.GetBalance(),
	}
}

