package streaming_compute

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
)

func (s *StreamingContext) GenerateVisualization(fname string) error {
	slog.Info("generate visualization", "path", fname)
	f, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	err = s.visualize(f)
	if err != nil {
		return err
	}

	return nil
}

func (s *StreamingContext) visualize(writer io.Writer) error {
	var err error

	_, err = writer.Write([]byte("stateDiagram-v2\n"))
	if err != nil {
		return err
	}

	for _, tableSeq := range s.tableSequences {
		if len(tableSeq) != 2 {
			return errors.New("sequence visualization error")
		}

		baseName := tableSeq[0].StreamTableName()
		dependName := tableSeq[1].StreamTableName()

		seq := fmt.Sprintf("\t%s--> %s\n", dependName, baseName)
		if !tableSeq[0].Temporary() {
			seq += fmt.Sprintf("\tstyle %s fill:#4CAF50\n", dependName)
		}

		if _, ok := tableSeq[1].(StreamingSource); ok {
			seq += fmt.Sprintf("\tstyle %s fill:#b56d02\n", dependName)
		}

		_, err = writer.Write([]byte(
			seq,
		))

	}

	return err
}
