package main

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
	"gorm.io/gorm"
)

type Table struct {
	Name string
}

type PostgresWriter struct {
	db *gorm.DB
}

// -------------------------------------

type ProcessHandler func(entry *accounting_core.JournalEntry) error

func NewProcessHandler(kv *stream_core.HashMapCounter) ProcessHandler {
	return func(entry *accounting_core.JournalEntry) error {
		var journalTeamID, accountTeamID uint64

		journalTeamID = uint64(entry.TeamID)
		accountTeamID = uint64(entry.Account.TeamID)

		key := fmt.Sprintf(
			"daily/%s/team/%d/account/%s/team/%d",
			entry.EntryTime.Format("2006-01-02"),
			entry.TeamID,
			entry.Account.AccountKey,
			entry.Account.TeamID,
		)

		kv.IncFloat64(key+"/debit", entry.Debit)
		kv.IncFloat64(key+"/credit", entry.Credit)

		switch entry.Account.BalanceType {
		case accounting_core.DebitBalance:
			kv.Merge(stream_core.MergeOpMin, reflect.Float64, key+"/balance",
				key+"/debit",
				key+"/credit",
			)
		case accounting_core.CreditBalance:
			kv.Merge(stream_core.MergeOpAdd, reflect.Float64, key+"/balance",
				key+"/credit",
				key+"/debit",
			)
		}

		acc := entry.Account

		switch acc.Coa {
		case accounting_core.ASSET:
			balance := entry.Debit - entry.Credit
			key := fmt.Sprintf(
				"daily/%s/team/%d/asset",
				entry.EntryTime.Format("2006-01-02"),
				entry.TeamID,
			)
			kv.IncFloat64(key, balance)
			// update general asset
			kv.IncFloat64("all/asset", balance)

		case accounting_core.LIABILITY:
			balance := entry.Credit - entry.Debit
			key := fmt.Sprintf(
				"daily/%s/team/%d/liability",
				entry.EntryTime.Format("2006-01-02"),
				entry.TeamID,
			)
			kv.IncFloat64(key, balance)
			// update general liability
			kv.IncFloat64("all/liability", balance)
		// case accounting_core.EQUITY:
		case accounting_core.EXPENSE:
			balance := entry.Debit - entry.Credit
			key := fmt.Sprintf(
				"daily/%s/team/%d/expense",
				entry.EntryTime.Format("2006-01-02"),
				entry.TeamID,
			)
			kv.IncFloat64(key, balance)
			// update general liability
			kv.IncFloat64("all/expense", balance)

		case accounting_core.REVENUE:
			balance := entry.Credit - entry.Debit
			key := fmt.Sprintf(
				"daily/%s/team/%d/revenue",
				entry.EntryTime.Format("2006-01-02"),
				entry.TeamID,
			)
			kv.IncFloat64(key, balance)
			// update general liability
			kv.IncFloat64("all/revenue", balance)
		}

		switch acc.AccountKey {
		case accounting_core.PayableAccount:
			recvKey := fmt.Sprintf(
				"daily/%s/team/%d/account/%s/team/%d/balance",
				entry.EntryTime.Format("2006-01-02"),
				accountTeamID,
				accounting_core.ReceivableAccount,
				journalTeamID,
			)

			diffkey := fmt.Sprintf(
				"daily/%s/team/%d/error/payable_diff/team/%d/amount",
				entry.EntryTime.Format("2006-01-02"),
				journalTeamID,
				accountTeamID,
			)

			kv.Merge(stream_core.MergeOpAdd, reflect.Float64, diffkey,
				key+"/balance",
				recvKey,
			)

		case accounting_core.ReceivableAccount:
			payKey := fmt.Sprintf(
				"daily/%s/team/%d/account/%s/team/%d/balance",
				entry.EntryTime.Format("2006-01-02"),
				accountTeamID,
				accounting_core.PayableAccount,
				journalTeamID,
			)

			diffkey := fmt.Sprintf(
				"daily/%s/team/%d/error/receivable_diff/team/%d/amount",
				entry.EntryTime.Format("2006-01-02"),
				journalTeamID,
				accountTeamID,
			)

			kv.Merge(stream_core.MergeOpAdd, reflect.Float64, diffkey,
				payKey,
				key+"/balance",
			)

		}

		return nil
	}
}

type PeriodicSnapshot func(t time.Time) error

func NewPeriodicSnapshot(kv *stream_core.HashMapCounter) PeriodicSnapshot {

	snapshotFunc := stream_utils.NewChainSnapshot(
		func(next stream_utils.NextFunc) stream_utils.NextFunc {
			return func(key string, kind reflect.Kind, value any) error {
				if value == 0 {
					return nil
				}
				if !strings.Contains(key, "payable_diff") {
					return nil
				}

				log.Printf("%s: %.3f\n", key, value)
				return next(key, kind, value)
			}
		},
	)

	return func(t time.Time) error {
		var err error

		// writer := stream_utils.CsvWriter{Data: map[string][][]string{}}

		log.Println("----------------------- snapshot ----------------------------")
		err = kv.Snapshot(t, snapshotFunc)
		log.Println("----------------------- end snapshot ------------------------")

		return err
	}
}
