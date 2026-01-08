package metric_daily

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/wargasipil/stream_engine/stream_core"
)

// jangan DIEDIT, file ini generate an dari package github.com/wargasipil/stream_engine

type MetricDailyTeamAccount struct {
	key string
	Name string
	store stream_core.KeyStore

	// Jangan Diubah letterlek
	Day string
	// Jangan Diubah letterlek
	TeamID uint64
	// Jangan Diubah letterlek
	Account string
}

func NewMetricDailyTeamAccount(store stream_core.KeyStore, Day string, TeamID uint64, Account string) *MetricDailyTeamAccount {
	keys := []string{}
	names := []string{}
	keys = append(keys, fmt.Sprintf("%s", Day))
	names = append(names, "day")
	keys = append(keys, fmt.Sprintf("%d", TeamID))
	names = append(names, "team")
	keys = append(keys, fmt.Sprintf("%s", Account))
	names = append(names, "account")
	key := fmt.Sprintf("%s/%s", strings.Join(names, "_"), strings.Join(keys, "/"))
	Name := strings.Join(names, "_")

	return &MetricDailyTeamAccount{
		store: store,
		Name: Name,
		key: key,
		Day: Day,
		TeamID: TeamID,
		Account: Account,
	}
}

func NewMetricDailyTeamAccountFromKey(store stream_core.KeyStore, mkey string) (*MetricDailyTeamAccount, error) {

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
	var Day string = indexkeys[0]
	var TeamID uint64
	TeamID, err = strconv.ParseUint(indexkeys[1], 10, 64)

	if err != nil {
		return nil, err
	}
	var Account string = indexkeys[2]

	return &MetricDailyTeamAccount{
		store: store,
		Name: Name,
		key: key,
		Day: Day,
		TeamID: TeamID,
		Account: Account,
	}, err
}

func IsMetricDailyTeamAccount(key string) bool {
	return strings.HasPrefix(key, "day_team_account/")
}

func (m *MetricDailyTeamAccount) PutDebit(value float64) float64 {
	return m.store.PutFloat64(m.key + "/debit", value)
}

func (m *MetricDailyTeamAccount) IncDebit(value float64) float64 {
	return m.store.IncFloat64(m.key + "/debit", value)
}

func (m *MetricDailyTeamAccount) GetDebit() float64 {
	return m.store.GetFloat64(m.key + "/debit")
}

func (m *MetricDailyTeamAccount) PutCredit(value float64) float64 {
	return m.store.PutFloat64(m.key + "/credit", value)
}

func (m *MetricDailyTeamAccount) IncCredit(value float64) float64 {
	return m.store.IncFloat64(m.key + "/credit", value)
}

func (m *MetricDailyTeamAccount) GetCredit() float64 {
	return m.store.GetFloat64(m.key + "/credit")
}

func (m *MetricDailyTeamAccount) PutBalance(value float64) float64 {
	return m.store.PutFloat64(m.key + "/balance", value)
}

func (m *MetricDailyTeamAccount) IncBalance(value float64) float64 {
	return m.store.IncFloat64(m.key + "/balance", value)
}

func (m *MetricDailyTeamAccount) GetBalance() float64 {
	return m.store.GetFloat64(m.key + "/balance")
}

func (m *MetricDailyTeamAccount) GetKey() string {
	return m.key
}

func (m *MetricDailyTeamAccount) Values() map[string]any {
	return map[string]any{
		"ID": stream_core.HashKeyString(m.key),
		"Day": m.Day,
		"TeamID": m.TeamID,
		"Account": m.Account,
		"Debit": m.GetDebit(),
		"Credit": m.GetCredit(),
		"Balance": m.GetBalance(),
	}
}

func (m *MetricDailyTeamAccount) Data() *DailyTeamAccount {
	return &DailyTeamAccount{
		ID: stream_core.HashKeyString(m.key),
		Day: m.Day,
		TeamID: m.TeamID,
		Account: m.Account,
		Debit: m.GetDebit(),
		Credit: m.GetCredit(),
		Balance: m.GetBalance(),
	}
}

func (m *MetricDailyTeamAccount) Any() any {
	return &DailyTeamAccount{
		ID: stream_core.HashKeyString(m.key),
		Day: m.Day,
		TeamID: m.TeamID,
		Account: m.Account,
		Debit: m.GetDebit(),
		Credit: m.GetCredit(),
		Balance: m.GetBalance(),
	}
}

type MetricDailyTeamToTeamAccount struct {
	key string
	Name string
	store stream_core.KeyStore

	// Jangan Diubah letterlek
	Day string
	// Jangan Diubah letterlek
	TeamID uint64
	// Jangan Diubah letterlek
	Account string
	// Jangan Diubah letterlek
	ToTeamID uint64
}

func NewMetricDailyTeamToTeamAccount(store stream_core.KeyStore, Day string, TeamID uint64, Account string, ToTeamID uint64) *MetricDailyTeamToTeamAccount {
	keys := []string{}
	names := []string{}
	keys = append(keys, fmt.Sprintf("%s", Day))
	names = append(names, "day")
	keys = append(keys, fmt.Sprintf("%d", TeamID))
	names = append(names, "team")
	keys = append(keys, fmt.Sprintf("%s", Account))
	names = append(names, "account")
	keys = append(keys, fmt.Sprintf("%d", ToTeamID))
	names = append(names, "toteam")
	key := fmt.Sprintf("%s/%s", strings.Join(names, "_"), strings.Join(keys, "/"))
	Name := strings.Join(names, "_")

	return &MetricDailyTeamToTeamAccount{
		store: store,
		Name: Name,
		key: key,
		Day: Day,
		TeamID: TeamID,
		Account: Account,
		ToTeamID: ToTeamID,
	}
}

func NewMetricDailyTeamToTeamAccountFromKey(store stream_core.KeyStore, mkey string) (*MetricDailyTeamToTeamAccount, error) {

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
	var Day string = indexkeys[0]
	var TeamID uint64
	TeamID, err = strconv.ParseUint(indexkeys[1], 10, 64)

	if err != nil {
		return nil, err
	}
	var Account string = indexkeys[2]
	var ToTeamID uint64
	ToTeamID, err = strconv.ParseUint(indexkeys[3], 10, 64)

	if err != nil {
		return nil, err
	}

	return &MetricDailyTeamToTeamAccount{
		store: store,
		Name: Name,
		key: key,
		Day: Day,
		TeamID: TeamID,
		Account: Account,
		ToTeamID: ToTeamID,
	}, err
}

func IsMetricDailyTeamToTeamAccount(key string) bool {
	return strings.HasPrefix(key, "day_team_account_toteam/")
}

func (m *MetricDailyTeamToTeamAccount) PutDebit(value float64) float64 {
	return m.store.PutFloat64(m.key + "/debit", value)
}

func (m *MetricDailyTeamToTeamAccount) IncDebit(value float64) float64 {
	return m.store.IncFloat64(m.key + "/debit", value)
}

func (m *MetricDailyTeamToTeamAccount) GetDebit() float64 {
	return m.store.GetFloat64(m.key + "/debit")
}

func (m *MetricDailyTeamToTeamAccount) PutCredit(value float64) float64 {
	return m.store.PutFloat64(m.key + "/credit", value)
}

func (m *MetricDailyTeamToTeamAccount) IncCredit(value float64) float64 {
	return m.store.IncFloat64(m.key + "/credit", value)
}

func (m *MetricDailyTeamToTeamAccount) GetCredit() float64 {
	return m.store.GetFloat64(m.key + "/credit")
}

func (m *MetricDailyTeamToTeamAccount) PutBalance(value float64) float64 {
	return m.store.PutFloat64(m.key + "/balance", value)
}

func (m *MetricDailyTeamToTeamAccount) IncBalance(value float64) float64 {
	return m.store.IncFloat64(m.key + "/balance", value)
}

func (m *MetricDailyTeamToTeamAccount) GetBalance() float64 {
	return m.store.GetFloat64(m.key + "/balance")
}

func (m *MetricDailyTeamToTeamAccount) GetKey() string {
	return m.key
}

func (m *MetricDailyTeamToTeamAccount) Values() map[string]any {
	return map[string]any{
		"ID": stream_core.HashKeyString(m.key),
		"Day": m.Day,
		"TeamID": m.TeamID,
		"Account": m.Account,
		"ToTeamID": m.ToTeamID,
		"Debit": m.GetDebit(),
		"Credit": m.GetCredit(),
		"Balance": m.GetBalance(),
	}
}

func (m *MetricDailyTeamToTeamAccount) Data() *DailyTeamToTeamAccount {
	return &DailyTeamToTeamAccount{
		ID: stream_core.HashKeyString(m.key),
		Day: m.Day,
		TeamID: m.TeamID,
		Account: m.Account,
		ToTeamID: m.ToTeamID,
		Debit: m.GetDebit(),
		Credit: m.GetCredit(),
		Balance: m.GetBalance(),
	}
}

func (m *MetricDailyTeamToTeamAccount) Any() any {
	return &DailyTeamToTeamAccount{
		ID: stream_core.HashKeyString(m.key),
		Day: m.Day,
		TeamID: m.TeamID,
		Account: m.Account,
		ToTeamID: m.ToTeamID,
		Debit: m.GetDebit(),
		Credit: m.GetCredit(),
		Balance: m.GetBalance(),
	}
}

