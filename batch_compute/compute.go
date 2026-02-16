package batch_compute

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"gorm.io/gorm"
)

type Table interface {
	TableName() string
	BuildQuery(graph *GraphContext) string
	Temporary() bool
}

type GlobalFilter struct {
	StartDate time.Time
}

type GraphContext struct {
	Schema           string
	seqMap           map[string]int
	sequences        []string
	tableSequences   [][]Table
	disableTemporary bool

	Filter *GlobalFilter
}

func NewGraphContext(schema string, disableTemporary bool, filter *GlobalFilter) *GraphContext {
	if schema == "public" {
		panic("schema utama coyyyyyyyyyyy")
	}
	return &GraphContext{schema, map[string]int{}, []string{}, [][]Table{}, disableTemporary, filter}
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

func (g *GraphContext) DependName(baseTable Table, table Table) string {

	tableName := g.GetTableName(table)

	if g.seqMap[tableName] == 0 {
		slog.Info("build dependent",
			"table_name", g.GetTableName(table),
			"temporary_table", table.Temporary(),
		)

		queries := g.BuildQueries(table)
		g.sequences = append(g.sequences, queries...)

	}
	g.tableSequences = append(g.tableSequences, []Table{table, baseTable})
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

func (g *GraphContext) GenerateVisualization(writer io.Writer, tables ...Table) error {
	var err error

	_, err = writer.Write([]byte("stateDiagram-v2\n"))
	if err != nil {
		return err
	}

	for _, table := range tables {

		// for triggering dependent
		tableName := g.GetTableName(table)
		if g.seqMap[tableName] != 0 {
			continue
		}

		table.BuildQuery(g)

		seq := fmt.Sprintf("\t%s--> [*]\n", table.TableName())
		if !table.Temporary() {
			seq += fmt.Sprintf("\tstyle %s fill:#4CAF50\n", table.TableName())
		}

		_, err = writer.Write([]byte(seq))
		if err != nil {
			return err
		}

	}

	for _, tableSeq := range g.tableSequences {
		if len(tableSeq) != 2 {
			return errors.New("sequence visualization error")
		}

		dependName := tableSeq[0].TableName()
		baseName := tableSeq[1].TableName()

		seq := fmt.Sprintf("\t%s--> %s\n", dependName, baseName)
		if !tableSeq[0].Temporary() {
			seq += fmt.Sprintf("\tstyle %s fill:#4CAF50\n", dependName)
		}

		_, err = writer.Write([]byte(
			seq,
		))

	}

	return err
}
