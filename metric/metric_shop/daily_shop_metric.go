package metric_shop

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/wargasipil/stream_engine/stream_core"
)

// jangan DIEDIT, file ini generate an dari package github.com/wargasipil/stream_engine

type MetricDailyShop struct {
	key string
	Name string
	store stream_core.KeyStore

	// Jangan Diubah letterlek
	Day string
	// Jangan Diubah letterlek
	ShopID uint64
	// Jangan Diubah letterlek
	Account string
}

func NewMetricDailyShop(store stream_core.KeyStore, Day string, ShopID uint64, Account string) *MetricDailyShop {
	keys := []string{}
	names := []string{}
	keys = append(keys, fmt.Sprintf("%s", Day))
	names = append(names, "day")
	keys = append(keys, fmt.Sprintf("%d", ShopID))
	names = append(names, "shop")
	keys = append(keys, fmt.Sprintf("%s", Account))
	names = append(names, "account")
	key := fmt.Sprintf("%s/%s", strings.Join(names, "_"), strings.Join(keys, "/"))
	Name := strings.Join(names, "_")

	return &MetricDailyShop{
		store: store,
		Name: Name,
		key: key,
		Day: Day,
		ShopID: ShopID,
		Account: Account,
	}
}

func NewMetricDailyShopFromKey(store stream_core.KeyStore, mkey string) (*MetricDailyShop, error) {

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
	var ShopID uint64
	ShopID, err = strconv.ParseUint(indexkeys[1], 10, 64)

	if err != nil {
		return nil, err
	}
	var Account string = indexkeys[2]

	return &MetricDailyShop{
		store: store,
		Name: Name,
		key: key,
		Day: Day,
		ShopID: ShopID,
		Account: Account,
	}, err
}

func IsMetricDailyShop(key string) bool {
	return strings.HasPrefix(key, "day_shop_account/")
}

func (m *MetricDailyShop) PutDebit(value float64) float64 {
	return m.store.PutFloat64(m.key + "/debit", value)
}

func (m *MetricDailyShop) IncDebit(value float64) float64 {
	return m.store.IncFloat64(m.key + "/debit", value)
}

func (m *MetricDailyShop) GetDebit() float64 {
	return m.store.GetFloat64(m.key + "/debit")
}

func (m *MetricDailyShop) PutCredit(value float64) float64 {
	return m.store.PutFloat64(m.key + "/credit", value)
}

func (m *MetricDailyShop) IncCredit(value float64) float64 {
	return m.store.IncFloat64(m.key + "/credit", value)
}

func (m *MetricDailyShop) GetCredit() float64 {
	return m.store.GetFloat64(m.key + "/credit")
}

func (m *MetricDailyShop) PutBalance(value float64) float64 {
	return m.store.PutFloat64(m.key + "/balance", value)
}

func (m *MetricDailyShop) IncBalance(value float64) float64 {
	return m.store.IncFloat64(m.key + "/balance", value)
}

func (m *MetricDailyShop) GetBalance() float64 {
	return m.store.GetFloat64(m.key + "/balance")
}

func (m *MetricDailyShop) PutLastBalance(value float64) float64 {
	return m.store.PutFloat64(m.key + "/last_balance", value)
}

func (m *MetricDailyShop) IncLastBalance(value float64) float64 {
	return m.store.IncFloat64(m.key + "/last_balance", value)
}

func (m *MetricDailyShop) GetLastBalance() float64 {
	return m.store.GetFloat64(m.key + "/last_balance")
}

func (m *MetricDailyShop) GetKey() string {
	return m.key
}

func (m *MetricDailyShop) Values() map[string]any {
	return map[string]any{
		"ID": stream_core.HashKeyString(m.key),
		"Day": m.Day,
		"ShopID": m.ShopID,
		"Account": m.Account,
		"Debit": m.GetDebit(),
		"Credit": m.GetCredit(),
		"Balance": m.GetBalance(),
		"LastBalance": m.GetLastBalance(),
	}
}

func (m *MetricDailyShop) Data() *DailyShop {
	return &DailyShop{
		ID: stream_core.HashKeyString(m.key),
		Day: m.Day,
		ShopID: m.ShopID,
		Account: m.Account,
		Debit: m.GetDebit(),
		Credit: m.GetCredit(),
		Balance: m.GetBalance(),
		LastBalance: m.GetLastBalance(),
	}
}

func (m *MetricDailyShop) Any() any {
	return &DailyShop{
		ID: stream_core.HashKeyString(m.key),
		Day: m.Day,
		ShopID: m.ShopID,
		Account: m.Account,
		Debit: m.GetDebit(),
		Credit: m.GetCredit(),
		Balance: m.GetBalance(),
		LastBalance: m.GetLastBalance(),
	}
}

type MetricShopLastBalanceDate struct {
	key string
	Name string
	store stream_core.KeyStore

	// Jangan Diubah letterlek
	ShopID uint64
	// Jangan Diubah letterlek
	Account string
}

func NewMetricShopLastBalanceDate(store stream_core.KeyStore, ShopID uint64, Account string) *MetricShopLastBalanceDate {
	keys := []string{}
	names := []string{}
	keys = append(keys, fmt.Sprintf("%d", ShopID))
	names = append(names, "shop")
	keys = append(keys, fmt.Sprintf("%s", Account))
	names = append(names, "account")
	key := fmt.Sprintf("%s/%s", strings.Join(names, "_"), strings.Join(keys, "/"))
	Name := strings.Join(names, "_")

	return &MetricShopLastBalanceDate{
		store: store,
		Name: Name,
		key: key,
		ShopID: ShopID,
		Account: Account,
	}
}

func NewMetricShopLastBalanceDateFromKey(store stream_core.KeyStore, mkey string) (*MetricShopLastBalanceDate, error) {

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
	var ShopID uint64
	ShopID, err = strconv.ParseUint(indexkeys[0], 10, 64)

	if err != nil {
		return nil, err
	}
	var Account string = indexkeys[1]

	return &MetricShopLastBalanceDate{
		store: store,
		Name: Name,
		key: key,
		ShopID: ShopID,
		Account: Account,
	}, err
}

func IsMetricShopLastBalanceDate(key string) bool {
	return strings.HasPrefix(key, "shop_account/")
}

func (m *MetricShopLastBalanceDate) PutLastUpdated(value int64) int64 {
	return m.store.PutInt64(m.key + "/last_updated", value)
}

func (m *MetricShopLastBalanceDate) IncLastUpdated(value int64) int64 {
	return m.store.IncInt64(m.key + "/last_updated", value)
}

func (m *MetricShopLastBalanceDate) GetLastUpdated() int64 {
	return m.store.GetInt64(m.key + "/last_updated")
}

func (m *MetricShopLastBalanceDate) GetKey() string {
	return m.key
}

func (m *MetricShopLastBalanceDate) Values() map[string]any {
	return map[string]any{
		"ID": stream_core.HashKeyString(m.key),
		"ShopID": m.ShopID,
		"Account": m.Account,
		"LastUpdated": m.GetLastUpdated(),
	}
}

func (m *MetricShopLastBalanceDate) Data() *ShopLastBalanceDate {
	return &ShopLastBalanceDate{
		ID: stream_core.HashKeyString(m.key),
		ShopID: m.ShopID,
		Account: m.Account,
		LastUpdated: m.GetLastUpdated(),
	}
}

func (m *MetricShopLastBalanceDate) Any() any {
	return &ShopLastBalanceDate{
		ID: stream_core.HashKeyString(m.key),
		ShopID: m.ShopID,
		Account: m.Account,
		LastUpdated: m.GetLastUpdated(),
	}
}

