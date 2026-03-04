package streaming_compute

import "gorm.io/gorm"

func MigrateSink(db *gorm.DB, schema string, dst ...interface{}) error {

	return db.Transaction(func(tx *gorm.DB) error {
		var err error
		err = tx.
			Exec("SET LOCAL search_path TO " + schema).
			Error

		if err != nil {
			return err
		}

		return tx.AutoMigrate(
			dst...,
		)
	})
}
