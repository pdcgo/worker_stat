package main

import (
	"github.com/pdcgo/shared/configs"
	"github.com/pdcgo/shared/db_connect"
	"gorm.io/gorm"
)

func NewDatabase(cfg *configs.AppConfig) (*gorm.DB, error) {
	return db_connect.NewProductionDatabase("stock-stat", &cfg.Database)
}
