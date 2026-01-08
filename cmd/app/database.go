package main

import (
	"github.com/pdcgo/shared/configs"
	"github.com/pdcgo/shared/db_connect"
	"github.com/pdcgo/worker_stat/metric/metric_daily"
	"github.com/pdcgo/worker_stat/metric/metric_team"
	"gorm.io/gorm"
)

func NewDatabase(cfg *configs.AppConfig) (*gorm.DB, error) {
	return db_connect.NewProductionDatabase("worker-stat", &cfg.Database)
}

type StatDatabase struct {
	DB *gorm.DB
}

func NewStatDatabase() (*StatDatabase, error) {
	db, err := db_connect.ConnectLocalDatabase()
	return &StatDatabase{
		DB: db,
	}, err
}

type Migrator func() error

func NewMigrator(statdb *StatDatabase) Migrator {
	return func() error {
		return statdb.DB.AutoMigrate(
			&metric_team.TeamAccount{},
			&metric_team.TeamLastBalance{},
			&metric_daily.DailyTeamAccount{},
			&metric_daily.DailyTeamToTeamAccount{},
		)
	}
}
