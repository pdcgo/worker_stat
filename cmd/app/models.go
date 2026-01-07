package main

import "time"

type BalanceAccountHistory struct {
	ID        uint `json:"id" gorm:"primarykey"`
	TeamID    uint `json:"team_id"`
	AccountID uint `json:"account_id" gorm:"index:account_at,unique"`

	Amount    float64   `json:"amount"`
	At        time.Time `json:"at" gorm:"index:account_at,unique"` // per day
	CreatedAt time.Time `json:"created_at"`
}
