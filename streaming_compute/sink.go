package streaming_compute

import "gorm.io/gorm"

type StreamingSink interface {
	Tabler
	IsSink() bool
}

func (s *StreamingContext) RegisterSink(db *gorm.DB, sink ...StreamingSink) error {
	var err error

	err = db.Transaction(func(tx *gorm.DB) error {
		for _, table := range sink {
			tableName := s.TableName(table)
			err = tx.Table(tableName).AutoMigrate(table)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}
