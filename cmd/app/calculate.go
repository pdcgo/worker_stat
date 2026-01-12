package main

import (
	"context"
	"log"
	"time"

	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/pdcgo/shared/pkg/debugtool"
	"github.com/urfave/cli/v3"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
	"gorm.io/gorm"
)

type CalculateFunc cli.ActionFunc

func NewCalculate(
	cfg *stream_core.CoreConfig,
	db *gorm.DB,
	process ProcessHandler,
	snapshot PeriodicSnapshot,
	calculateBalance CalculateBalanceHistory,
	kv stream_core.KeyStore,

) CalculateFunc {
	return func(ctx context.Context, c *cli.Command) error {
		var err error
		// err = kv.ResetCounter()
		// if err != nil {
		// 	log.Fatal(err)
		// }

		// duration := time.Second * 30
		// tick := time.NewTimer(time.Minute * 15)

		last := time.Now()
		// go func() {
		// 	for {
		// 		select {
		// 		case <-tick.C:
		// 			err = snapshot(last)
		// 			if err != nil {
		// 				slog.Error(err.Error())
		// 			}
		// 			tick.Reset(duration)
		// 			last = time.Now()
		// 		case <-context.Background().Done():
		// 			return
		// 		}
		// 	}
		// }()

		// iterating journal entries
		var entries []*accounting_core.JournalEntry
		for {

			pkey := kv.GetUint64("accounting_pkey")
			entries = []*accounting_core.JournalEntry{}
			err = db.
				Model(&accounting_core.JournalEntry{}).
				Joins("join transactions on journal_entries.transaction_id = transactions.id").
				Preload("Account").
				Preload("Transaction").
				Where("transactions.created > ?", "2026-01-05").
				Where("journal_entries.id > ?", pkey).
				Limit(5000).
				// Limit(5).
				Order("id asc").
				Find(&entries).
				Error

			if err != nil {
				log.Fatal(err)
			}

			// teamIDs := []uint{}
			// for _, entry := range entries {
			// 	teamIDs = append(teamIDs, entry.Transaction.TeamID, entry.Account.TeamID)
			// }

			// slog.Info("calculate daily Balance")
			// err = calculateBalance(teamIDs)
			// if err != nil {
			// 	return err
			// }

			if len(entries) == 0 {
				err = snapshot(last)
				if err != nil {
					log.Fatal(err)
				}
				last = time.Now()
				// tick.Reset(time.Second)
				calculateBalance()
				time.Sleep(time.Minute)
			}

			for _, entry := range entries {
				err = process(entry)
				if err != nil {
					log.Fatal(err)
				}

				kv.PutUint64("accounting_pkey", uint64(entry.ID))

			}

			// test
			// break
		}

		// return nil
	}
}

type CalculateBalanceHistory func() error

type Balance struct {
	TeamID      uint    `json:"team_id"`
	Amount      float64 `json:"amount"`
	DateAt      string  `json:"date_at"`
	AccountType string  `json:"account_type"`
}

func NewCalculateBalanceHistory(
	db *gorm.DB,
	kv stream_core.KeyStore,
) CalculateBalanceHistory {

	balances := []*Balance{}
	process := stream_utils.NewChain(
		DailyBalance(kv),
	)

	return func() error {
		// ids := []uint{}
		// mapid := map[uint]bool{}

		// for _, teamID := range teamIDs {
		// 	if mapid[teamID] {
		// 		continue
		// 	}
		// 	mapid[teamID] = true
		// 	ids = append(ids, teamID)
		// }

		err := db.Raw(`
			SELECT 
				date(bah.at) as date_at,
				bah.team_id as team_id,
				aty.type as account_type,
				sum(bah.amount) as amount
			from balance_account_histories bah
			join expense_accounts ea on ea.id = bah.account_id 
			join account_types aty on aty.id = ea.account_type_id 
			where 
				bah.at > ?
				-- and bah.team_id in ?
			group by 
				date_at, bah.team_id, aty.type
		`, time.Now().AddDate(0, 0, -7)).
			Find(&balances).
			Error

		if err != nil {
			return err
		}

		for _, balance := range balances {
			err = process(balance)
			if err != nil {
				return err
			}
		}

		return nil
	}
}

func DailyBalance(kv stream_core.KeyStore) stream_utils.ChainNextHandler[*Balance] {
	return func(next stream_utils.ChainNextFunc[*Balance]) stream_utils.ChainNextFunc[*Balance] {
		return func(data *Balance) error {
			// var err error
			// err = kv.Transaction(func(tx *stream_core.Transaction) error {
			// 	metric := metric_team.NewMetricTeamLastBalance(tx, data.DateAt)
			// 	return nil
			// })

			debugtool.LogJson(data)

			return next(data)
		}
	}
}
