package streaming_compute

import (
	"context"
	"fmt"
	"strings"
)

type StreamingStep interface{}

type StreamingContext struct {
	ctx context.Context
}

func NewStreamingContext() *StreamingContext {
	return &StreamingContext{}
}

func (s *StreamingContext) Compute() error {
	return nil
}

func Upsert(
	dstTable string,
	tableName string,
	onConflict []string,
	fields []string,
) string {

	sets := []string{}
	for _, field := range fields {
		sets = append(sets,
			fmt.Sprintf("%s = EXCLUDED.%s", field, field),
		)
	}

	return fmt.Sprintf(
		`
		INSERT INTO %s (%s)
		SELECT * FROM %s
		ON CONFLICT (%s)
		DO UPDATE SET
			%s
		`,
		dstTable,
		strings.Join(fields, ", "),
		tableName,
		strings.Join(onConflict, ", "),
		strings.Join(sets, ","),
	)
}
