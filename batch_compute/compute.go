package batch_compute

import (
	"context"
	"fmt"
	"strings"
	"text/template"

	"gorm.io/gorm"
)

type Schema string

func (s Schema) GetTableName(table Table) string {
	if table.Temporary() {
		return table.TableName()
	}
	return string(s) + "." + table.TableName()
}

type GraphContext struct {
	Schema Schema
}

func (g *GraphContext) DependName(table Table) string {
	if table.Temporary() {
		return table.TableName()
	}
	return string(g.Schema) + "." + table.TableName()
}

type Table interface {
	TableName() string
	CreateQuery(schema Schema) string
	DependsTable() []Table
	Temporary() bool
}

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
	depends := table.DependsTable()
	for _, depend := range depends {
		err = c.computeTable(ctx, depend)
		if err != nil {
			return err
		}

	}

	// execute table
	c.tableMap[tableName] = table

	query := table.CreateQuery(c.schema)
	if c.schema == "" || table.Temporary() {
		query = fmt.Sprintf("create temp table %s as\n %s", tableName, query)
	} else {
		dropq := fmt.Sprintf("drop table if exists %s.%s", c.schema, tableName)
		err = c.tx.Exec(dropq).Error
		if err != nil {
			return err
		}

		query = fmt.Sprintf("create table %s.%s as\n %s", c.schema, tableName, query)
	}

	err = c.tx.Exec(query).Error
	if err != nil {
		return err
	}

	return nil
}

func GetTableName(table Table) string {
	if table.Temporary() {
		return table.TableName()
	}
	return "public." + table.TableName()

}
