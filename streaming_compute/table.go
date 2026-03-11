package streaming_compute

import (
	"fmt"
	"log/slog"
	"strings"

	"gorm.io/gorm"
)

type Tabler interface {
	StreamTableName() string
	Temporary() bool
}

type StreamingTable interface {
	Tabler
	BuildQueries(s *StreamingContext) []string
}

func (s *StreamingContext) createTable(
	tx *gorm.DB,
	table StreamingTable,
) error {
	var err error

	stmt := &gorm.Statement{DB: tx}
	err = stmt.ParseWithSpecialTableName(table, stmt.Table)
	if err != nil {
		return err
	}

	if len(stmt.Schema.DBNames) == 0 {
		return fmt.Errorf("table %s has no fields", table.StreamTableName())
	}

	var fields []string
	var fieldnames []string
	for _, dbName := range stmt.Schema.DBNames {
		field := stmt.Schema.FieldsByDBName[dbName]
		if !field.IgnoreMigration {
			dbField := tx.Migrator().FullDataTypeOf(field)

			fields = append(fields, fmt.Sprintf("%s %s", dbName, dbField.SQL))
			fieldnames = append(fieldnames, dbName)
		}
	}

	var createCommand string
	var tableName string

	var queries []string = []string{}

	if s.disableTemporary == false &&
		table.Temporary() {

		createCommand = "create temp table"
		tableName = table.StreamTableName()
		queries = append(queries,
			fmt.Sprintf(
				`
					%s %s (
						%s
					) on commit drop;
				`,
				createCommand,
				tableName,
				strings.Join(fields, ", \n"),
			),
		)

	} else {

		createCommand = "create table if not exists"
		tableName = fmt.Sprintf("%s.%s", s.schema, table.StreamTableName())
		queries = append(queries,
			fmt.Sprintf(
				`
					%s %s (
						%s
					);
				`,
				createCommand,
				tableName,
				strings.Join(fields, ", \n"),
			),
			fmt.Sprintf(`truncate table %s`, tableName),
		)
	}

	for _, query := range queries {
		slog.Debug("create",
			"table", table.StreamTableName(),
			"query", query,
		)
		err = tx.Exec(query).Error
		if err != nil {
			return err
		}
	}

	s.tableFieldMap[s.TableName(table)] = fieldnames

	return nil
}

type StreamingExternal struct {
	Name string
}

// StreamTableName implements [Tabler].
func (s *StreamingExternal) StreamTableName() string {
	return s.Name
}

// Temporary implements [Tabler].
func (s *StreamingExternal) Temporary() bool {
	return false
}
