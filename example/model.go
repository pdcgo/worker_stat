package example

import (
	"github.com/pdcgo/shared/db_models"
)

//go:generate code_gen
type SkuReadyStockExample struct {
	SkuID        db_models.SkuID `gorm:"primarykey"`
	ReadyCount   int64
	OngoingCount int64
}
