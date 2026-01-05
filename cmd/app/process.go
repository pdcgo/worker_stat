package main

import (
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
)

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
		case accounting_core.LIABILITY:
			balance := entry.Credit - entry.Debit
			key := fmt.Sprintf(
				"daily/%s/team/%d/liability",
				entry.EntryTime.Format("2006-01-02"),
				entry.TeamID,
			)
			kv.IncFloat64(key, balance)
		}

		switch acc.AccountKey {
		case accounting_core.PayableAccount:
			pay := kv.GetFloat64(key + "/balance")
			recvKey := fmt.Sprintf(
				"daily/%s/team/%d/%s/%d/balance",
				entry.EntryTime.Format("2006-01-02"),
				accountTeamID,
				accounting_core.ReceivableAccount,
				journalTeamID,
			)
			recv := kv.GetFloat64(recvKey)

			diff := pay + recv

			if diff != 0 {
				diffkey := fmt.Sprintf(
					"daily/%s/team/%d/payable_diff/%d/amount",
					entry.EntryTime.Format("2006-01-02"),
					journalTeamID,
					accountTeamID,
				)

				kv.PutFloat64(diffkey, diff)
			}

		case accounting_core.ReceivableAccount:
			pay := kv.GetFloat64(key + "/balance")
			recvKey := fmt.Sprintf(
				"daily/%s/team/%d/%s/%d/balance",
				entry.EntryTime.Format("2006-01-02"),
				accountTeamID,
				accounting_core.ReceivableAccount,
				journalTeamID,
			)
			recv := kv.GetFloat64(recvKey)

			diff := pay - recv

			if diff != 0 {
				diffkey := fmt.Sprintf(
					"daily/%s/team/%d/payable_diff/%d/amount",
					entry.EntryTime.Format("2006-01-02"),
					journalTeamID,
					accountTeamID,
				)

				kv.PutFloat64(diffkey, diff)
			}

		}

		return nil
	}
}

type PeriodicSnapshot func(t time.Time) error

func NewPeriodicSnapshot(kv *stream_core.HashMapCounter) PeriodicSnapshot {

	db := stream_utils.NewDatabaseLocal()

	return func(t time.Time) error {
		var err error

		// writer := stream_utils.CsvWriter{Data: map[string][][]string{}}

		log.Println("----------------------- snapshot ----------------------------")
		err = kv.Snapshot(t, func(key string, kind reflect.Kind, value any) error {
			log.Printf("%s: %.3f\n", key, value)
			// writer.Write(key, kind, value)
			err = stream_utils.WriteToDatabase(db, key, value)

			if err != nil {
				panic(err)
			}
			return err
		})
		log.Println("----------------------- end snapshot ------------------------")

		return err
	}
}
