package batch_compute

import (
	"context"
	"fmt"
	"log/slog"

	"gorm.io/gorm"
)

type Table interface {
	TableName() string
	BuildQuery(graph *GraphContext) string
	Temporary() bool
}

type GraphContext struct {
	Schema           string
	seqMap           map[string]int
	sequences        []string
	disableTemporary bool
}

func NewGraphContext(schema string, disableTemporary bool) *GraphContext {
	if schema == "public" {
		panic("schema utama coyyyyyyyyyyy")
	}
	return &GraphContext{schema, map[string]int{}, []string{}, disableTemporary}
}

func (g *GraphContext) GetTableName(table Table) string {
	if g.disableTemporary {
		return string(g.Schema) + "." + table.TableName()
	}

	if table.Temporary() {
		return table.TableName()
	}
	return string(g.Schema) + "." + table.TableName()
}

func (g *GraphContext) DependName(table Table) string {

	tableName := g.GetTableName(table)

	if g.seqMap[tableName] == 0 {
		slog.Info("build dependent",
			"table_name", g.GetTableName(table),
			"temporary_table", table.Temporary(),
		)

		queries := g.BuildQueries(table)
		g.sequences = append(g.sequences, queries...)
	}

	g.seqMap[tableName] += 1

	return tableName
}

func (g *GraphContext) BuildQueries(table Table) []string {

	tableName := table.TableName()
	queries := []string{}

	query := table.BuildQuery(g)

	if g.disableTemporary == false && (g.Schema == "" || table.Temporary()) {
		query = fmt.Sprintf("create temp table %s as\n %s", tableName, query)
		queries = append(queries, query)
	} else {
		dropq := fmt.Sprintf("drop table if exists %s.%s", g.Schema, tableName)
		query = fmt.Sprintf("create table %s.%s as\n %s", g.Schema, tableName, query)

		queries = append(queries, dropq, query)
	}

	return queries
}

func (g *GraphContext) Compute(ctx context.Context, tx *gorm.DB, tables ...Table) error {
	var err error

	computeQueries := []string{}

	for _, table := range tables {
		tableName := g.GetTableName(table)
		if g.seqMap[tableName] != 0 {
			continue
		}

		slog.Info("build query", "table_name", tableName)
		computeQueries = append(computeQueries, g.BuildQueries(table)...)
	}

	for _, query := range g.sequences {
		err = tx.Exec(query).Error
		if err != nil {
			return err
		}
	}

	for _, query := range computeQueries {
		err = tx.Exec(query).Error
		if err != nil {
			return err
		}
	}

	return err

}
