package streaming_compute

import (
	"gorm.io/gorm"
)

type SourceTable interface {
	TableName() string
	AfterCalculate(db *gorm.DB) error
}

func NewSource[T SourceTable](db *gorm.DB, schema string, source T) (func(data T) error, error) {
	var err error
	var handler func(data T) error

	// tableName := ta.TableName()
	// if strings.HasPrefix(tableName, "public.") {
	// 	return handler, fmt.Errorf("tidak boleh pakai schema public  %s", tableName)
	// }

	// if !strings.Contains(tableName, ".") {
	// 	return handler, fmt.Errorf("add schema explicitly %s", tableName)
	// }
	tableName := schema + "." + source.TableName()
	err = db.
		Table(tableName).
		AutoMigrate(source)
	if err != nil {
		return handler, err
	}

	handler = func(data T) error {
		return db.Table(tableName).Save(data).Error
	}

	return handler, err
}
