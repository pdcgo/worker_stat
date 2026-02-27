package streaming_compute

import (
	"context"
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
