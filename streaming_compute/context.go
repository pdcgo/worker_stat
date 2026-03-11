package streaming_compute

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"gorm.io/gorm"
)

type ComputeFunc func(ctx context.Context, tx *gorm.DB) error

type StreamingAfterCompute interface {
	AfterCompute(s *StreamingContext) ComputeFunc
}

type StreamingContext struct {

	// ctx              context.Context
	// db               *gorm.DB
	schema           string
	disableTemporary bool

	// perkara locking
	lock sync.Mutex

	// query sequence
	sequencesQ     []string
	afterSequenceQ []string
	tableSequences [][]Tabler

	// untuk add
	tableMap       map[string]StreamingTable
	externalMap    map[string]Tabler
	tableFieldMap  map[string][]string
	sourceTablemap map[string]StreamingSource
}

type Option func(sctx *StreamingContext)

func WithSchemaOption(schema string) Option {
	return func(sctx *StreamingContext) {
		sctx.schema = schema
	}
}

func WithDisableTemporary() Option {
	return func(sctx *StreamingContext) {
		sctx.disableTemporary = true
	}
}

func NewStreamingContext(options ...Option) *StreamingContext {
	sctx := &StreamingContext{
		tableMap:       map[string]StreamingTable{},
		tableFieldMap:  map[string][]string{},
		sourceTablemap: map[string]StreamingSource{},
		externalMap:    map[string]Tabler{},
	}

	for _, opt := range options {
		opt(sctx)
	}

	return sctx
}

func (s *StreamingContext) Empty() {
	s.sequencesQ = []string{}
	s.tableSequences = [][]Tabler{}
	s.tableMap = map[string]StreamingTable{}

}

func (s *StreamingContext) TableName(table Tabler) string {
	var tname string
	if s.disableTemporary == false &&
		table.Temporary() {
		tname = table.StreamTableName()
	} else {
		tname = fmt.Sprintf("%s.%s", s.schema, table.StreamTableName())
	}
	return tname
}

func (s *StreamingContext) DependTable(dst StreamingTable, depend StreamingTable) string {
	dependTable := s.TableName(depend)

	if s.tableMap[dependTable] == nil {
		slog.Info("build dependent",
			"table_name", dependTable,
			"temporary_table", depend.Temporary(),
		)

		s.sequencesQ = append(s.sequencesQ, depend.BuildQueries(s)...)
		s.tableMap[dependTable] = depend
	}

	s.tableSequences = append(s.tableSequences, []Tabler{
		dst, depend,
	})

	return dependTable
}

func (s *StreamingContext) DependSource(dst StreamingTable, depend StreamingSource) string {
	dependTable := s.TableName(depend)

	if s.sourceTablemap[dependTable] == nil {
		slog.Info("build dependent",
			"source_table", dependTable,
		)

		s.sourceTablemap[dependTable] = depend
	}

	s.tableSequences = append(s.tableSequences, []Tabler{
		dst, depend,
	})

	return dependTable
}

func (s *StreamingContext) DependExternal(dst Tabler, external string) string {
	depend := &StreamingExternal{
		Name: external,
	}

	dependTable := depend.StreamTableName()

	if s.externalMap[dependTable] == nil {
		slog.Info("build dependent",
			"table_name", dependTable,
		)

		s.externalMap[dependTable] = depend
	}

	s.tableSequences = append(s.tableSequences, []Tabler{
		dst, depend,
	})

	return dependTable
}

func (s *StreamingContext) Lock() {
	s.lock.Lock()
}

func (s *StreamingContext) Unlock() {
	s.lock.Unlock()
}
