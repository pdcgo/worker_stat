package main

import (
	"github.com/pdcgo/shared/configs"
	"github.com/pdcgo/shared/db_connect"
	"gorm.io/gorm"
)

func NewProductionDatabase(cfg *configs.AppConfig) (*gorm.DB, error) {
	return db_connect.NewProductionDatabase("batch_processing", &cfg.Database)
}
