package streaming_compute

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

type StreamingStep interface{}

type StreamingContext struct {
	ctx    context.Context
	db     *gorm.DB
	schema string
}

func NewStreamingContext(ctx context.Context, db *gorm.DB, schema string) *StreamingContext {
	return &StreamingContext{ctx, db, schema}
}

func (s *StreamingContext) SetSchema(tx *gorm.DB) error {
	return tx.Exec("SET LOCAL search_path TO " + s.schema).Error
}

type Tabler interface {
	TableName() string
}

func NewSource[T any](db *gorm.DB, schema string, source T) (func(data T) error, error) {
	var err error
	var handler func(data T) error
	var dd any = source

	ta, ok := dd.(Tabler)
	if !ok {
		return handler, errors.New("unsupported for creating source")
	}

	// tableName := ta.TableName()
	// if strings.HasPrefix(tableName, "public.") {
	// 	return handler, fmt.Errorf("tidak boleh pakai schema public  %s", tableName)
	// }

	// if !strings.Contains(tableName, ".") {
	// 	return handler, fmt.Errorf("add schema explicitly %s", tableName)
	// }
	tableName := schema + "." + ta.TableName()
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

func (s *StreamingContext) Compute() error {
	return nil
}

func Upsert(
	dstTable string,
	query string,
	onConflict []string,
	fields []string,
) string {

	sets := []string{}
	for _, field := range fields {
		sets = append(sets,
			fmt.Sprintf("%s = EXCLUDED.%s", field, field),
		)
	}

	allFields := []string{}
	allFields = append(allFields, onConflict...)
	allFields = append(allFields, fields...)

	return fmt.Sprintf(
		`
		INSERT INTO %s (%s)
		%s
		ON CONFLICT (%s)
		DO UPDATE SET
			%s
		`,
		dstTable,
		strings.Join(allFields, ", "),
		query,
		strings.Join(onConflict, ", "),
		strings.Join(sets, ","),
	)
}
