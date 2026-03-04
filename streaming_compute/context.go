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
	tableSequences [][]StreamingTable

	// untuk add
	tableMap       map[string]StreamingTable
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
	}

	for _, opt := range options {
		opt(sctx)
	}

	return sctx
}

func (s *StreamingContext) Empty() {
	s.sequencesQ = []string{}
	s.tableSequences = [][]StreamingTable{}
	s.tableMap = map[string]StreamingTable{}

}

func (s *StreamingContext) Compute(tables ...StreamingTable) ComputeFunc {

	s.Empty()

	// building Query
	var queries []string
	for _, table := range tables {
		tname := s.TableName(table)
		if s.tableMap[tname] != nil {
			continue
		}

		slog.Debug("build streaming table", "table_name", tname)
		queries = table.BuildQueries(s)
		s.sequencesQ = append(s.sequencesQ, queries...)
		s.tableMap[tname] = table
	}

	tableMap := s.tableMap
	sequence := s.sequencesQ

	var beforeComputed ComputeFunc = func(_ context.Context, tx *gorm.DB) error {
		var err error

		for _, table := range tableMap {
			slog.Debug("creating", "table", table.StreamTableName())
			err = s.createTable(tx, table)
			if err != nil {
				return err
			}
		}

		return nil
	}

	var afterComputed ComputeFunc = func(_ context.Context, tx *gorm.DB) error {
		var err error
		for _, source := range s.sourceTablemap {
			tableName := s.TableName(source)
			slog.Debug("truncating",
				"source", tableName,
			)
			err = tx.Exec(fmt.Sprintf(`truncate table %s`, tableName)).Error
			if err != nil {
				return err
			}
		}
		return nil
	}

	return func(ctx context.Context, tx *gorm.DB) error {
		var err error

		s.lock.Lock()
		defer s.lock.Unlock()

		// running before computed
		err = beforeComputed(ctx, tx)
		if err != nil {
			return err
		}

		// running computation
		for _, query := range sequence {
			slog.Debug(query)
			err = tx.Exec(query).Error
			if err != nil {
				return err
			}
		}

		// running before computed
		err = afterComputed(ctx, tx)
		if err != nil {
			return err
		}

		return err
	}

}

func (s *StreamingContext) TableName(table Tabler) string {
	return fmt.Sprintf("%s.%s", s.schema, table.StreamTableName())
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

	s.tableSequences = append(s.tableSequences, []StreamingTable{
		dst, depend,
	})

	return dependTable
}

// func (s *StreamingContext) addTableMap(table StreamingContext) {}
