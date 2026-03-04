package streaming_compute

import (
	"fmt"
	"strings"
)

func Upsert(
	schema string,
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
		INSERT INTO %s.%s (%s)
		%s
		ON CONFLICT (%s)
		DO UPDATE SET
			%s
		`,
		schema,
		dstTable,
		strings.Join(allFields, ", "),
		query,
		strings.Join(onConflict, ", "),
		strings.Join(sets, ","),
	)
}
