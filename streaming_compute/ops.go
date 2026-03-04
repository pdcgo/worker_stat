package streaming_compute

import "fmt"

func (s *StreamingContext) Istemporary(table StreamingTable) bool {
	return s.disableTemporary == false && table.Temporary()
}

func (s *StreamingContext) InsertOps(table StreamingTable, query string) string {
	var tableName string
	if s.Istemporary(table) {
		tableName = table.StreamTableName()
	} else {
		tableName = s.TableName(table)
	}

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
