package metric_daily

import (
	"fmt"

	"github.com/pdcgo/accounting_service/accounting_core"
	"github.com/wargasipil/stream_engine/stream_core"
	"github.com/wargasipil/stream_engine/stream_utils"
)

func DailyLiability(kv *stream_core.HashMapCounter) stream_utils.ChainNextHandler[*accounting_core.JournalEntry] {
	return func(next stream_utils.ChainNextFunc[*accounting_core.JournalEntry]) stream_utils.ChainNextFunc[*accounting_core.JournalEntry] {
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

			acc := entry.Account

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

				err := kv.Transaction(func(tx *stream_core.Transaction) error {
					tx.PutFloat64(diffkey, tx.GetFloat64(key+"/balance")+tx.GetFloat64(recvKey))

					return nil
				})

				if err != nil {
					return err
				}

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

				err := kv.Transaction(func(tx *stream_core.Transaction) error {
					tx.PutFloat64(diffkey, tx.GetFloat64(payKey)+tx.GetFloat64(key+"/balance"))

					return nil
				})

				if err != nil {
					return err
				}

			}

			return next(entry)
		}
	}
}
