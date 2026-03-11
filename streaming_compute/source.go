package streaming_compute

import (
	"gorm.io/gorm"
)

type StreamingSource interface {
	Tabler
	IsSource() bool
}

func (s *StreamingContext) EmitToSource(tx *gorm.DB, row StreamingSource) error {
	if !row.IsSource() {
		return nil
	}

	// s.lock.Lock()
	// defer s.lock.Unlock()

	err := tx.
		Table(s.TableName(row)).
		Create(row).
		Error

	return err
}

func (s *StreamingContext) RegisterSource(db *gorm.DB, sources ...StreamingSource) error {
	var err error
	return db.Transaction(func(tx *gorm.DB) error {
		for _, source := range sources {
			tableName := s.TableName(source)
			err = tx.Table(tableName).AutoMigrate(&source)
			if err != nil {
				return err
			}

			s.sourceTablemap[tableName] = source
		}
		return nil
	})
}
