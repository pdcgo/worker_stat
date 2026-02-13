package batch_compute

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"text/template"

	"gorm.io/gorm"
)

type Table interface {
	TableName() string
	BuildQuery(graph *GraphContext) string
	Temporary() bool
}

type Schema string

func (s Schema) GetTableName(table Table) string {
	if table.Temporary() {
		return table.TableName()
	}
	return string(s) + "." + table.TableName()
}

type GraphContext struct {
	Schema    Schema
	seqMap    map[string]int
	sequences []Table
}

func NewGraphContext(schema Schema) *GraphContext {
	if schema == "public" {
		panic("schema utama coyyyyyyyyyyy")
	}
	return &GraphContext{schema, map[string]int{}, []Table{}}
}

func (g *GraphContext) DependName(table Table) string {

	tableName := g.Schema.GetTableName(table)

	if g.seqMap[tableName] == 0 {
		g.sequences = append(g.sequences, table)

	}

	g.seqMap[tableName] += 1

	return tableName
}

func (g *GraphContext) BuildQueries(table Table) []string {

	tableName := table.TableName()
	queries := []string{}

	query := table.BuildQuery(g)
	if g.Schema == "" || table.Temporary() {
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
		tableName := g.Schema.GetTableName(table)
		if g.seqMap[tableName] != 0 {
			continue
		}

		slog.Info("build query", "table_name", tableName)

		computeQueries = append(computeQueries, g.BuildQueries(table)...)
	}

	for _, table := range g.sequences {
		slog.Info("compute dependent",
			"table_name", g.Schema.GetTableName(table),
			"temporary_table", table.Temporary(),
			"level", g.seqMap[g.Schema.GetTableName(table)],
		)

		queries := g.BuildQueries(table)
		for _, query := range queries {
			err = tx.Exec(query).Error
			if err != nil {
				return err
			}
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

// ------------------------- konsep lama -----------------------------------

func NewTableSelect(tableName string, query string, dependsTable map[string]Table) Table {
	return &table{
		tableName:    tableName,
		query:        query,
		dependsTable: dependsTable,
	}
}

func NewTemporarySelect(tableName string, query string, dependsTable map[string]Table) Table {
	return &table{
		temporary:    true,
		tableName:    tableName,
		query:        query,
		dependsTable: dependsTable,
	}
}

type table struct {
	temporary    bool
	tableName    string
	query        string
	dependsTable map[string]Table
}

// BuildQuery implements [Table].
func (t *table) BuildQuery(graph *GraphContext) string {
	panic("unimplemented")
}

// Temporary implements Table.
func (t *table) Temporary() bool {
	return t.temporary
}

func (t *table) TableName() string {
	return t.tableName
}

func (t *table) CreateQuery(schema Schema) string {
	tem := template.Must(template.New(t.tableName).Parse(t.query))
	var sb strings.Builder

	tableValue := map[string]string{}
	for key, table := range t.dependsTable {
		tableValue[key+"Table"] = string(schema) + "." + table.TableName()
	}

	err := tem.Execute(&sb, tableValue)
	if err != nil {
		panic(err)
	}
	return sb.String()
}

func (t *table) DependsTable() []Table {
	depends := make([]Table, 0, len(t.dependsTable))
	for _, table := range t.dependsTable {
		depends = append(depends, table)
	}
	return depends
}

type Compute struct {
	tx       *gorm.DB
	tableMap map[string]Table
	schema   Schema
}

func NewCompute(tx *gorm.DB, schema Schema) *Compute {
	if schema == "public" {
		panic("schema utama coyyyyyyyyyyy")
	}
	return &Compute{tx, map[string]Table{}, schema}
}

func (c *Compute) Compute(ctx context.Context, tables ...Table) error {
	var err error
	for _, table := range tables {
		err = c.computeTable(ctx, table)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Compute) computeTable(ctx context.Context, table Table) error {
	var err error
	tableName := table.TableName()
	// skip jika sudah ada
	if _, ok := c.tableMap[tableName]; ok {
		return nil
	}

	// executing depend first
	// depends := table.DependsTable()
	// for _, depend := range depends {
	// 	err = c.computeTable(ctx, depend)
	// 	if err != nil {
	// 		return err
	// 	}

	// }

	// execute table
	// c.tableMap[tableName] = table

	// query := table.CreateQuery(c.schema)
	// if c.schema == "" || table.Temporary() {
	// 	query = fmt.Sprintf("create temp table %s as\n %s", tableName, query)
	// } else {
	// 	dropq := fmt.Sprintf("drop table if exists %s.%s", c.schema, tableName)
	// 	err = c.tx.Exec(dropq).Error
	// 	if err != nil {
	// 		return err
	// 	}

	// 	query = fmt.Sprintf("create table %s.%s as\n %s", c.schema, tableName, query)
	// }

	// err = c.tx.Exec(query).Error
	// if err != nil {
	// 	return err
	// }

	return err
}

func GetTableName(table Table) string {
	if table.Temporary() {
		return table.TableName()
	}
	return "public." + table.TableName()

}
