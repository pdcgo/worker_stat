package streaming_compute

import (
	"context"
	"fmt"
	"log/slog"

	"gorm.io/gorm"
)

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
			if _, ok := table.(StreamingSink); ok {
				continue
			}

			slog.Debug("creating", "table", s.TableName(table))
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
