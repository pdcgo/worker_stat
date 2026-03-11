package streaming_compute

import (
	"fmt"
	"strings"
)

func (s *StreamingContext) Istemporary(table StreamingTable) bool {
	return s.disableTemporary == false && table.Temporary()
}

func (s *StreamingContext) InsertOps(table StreamingTable, query string) string {
	var tableName string = s.TableName(table)

	return fmt.Sprintf(
		`
		insert into %s (
			%s
		)
		`,
		tableName,
		query,
	)
}

type UpsertPayload struct {
	DestinationTable string
	Query            string
	OnConflict       []string
	Fields           []string
}

func (s *StreamingContext) UpsertOps(pay *UpsertPayload) string {

	dstTable := pay.DestinationTable
	query := pay.Query
	onConflict := pay.OnConflict
	fields := pay.Fields

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
