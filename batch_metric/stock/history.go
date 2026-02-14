package stock

import (
	"github.com/pdcgo/worker_stat/batch_compute"
)

type InvHistoryLog struct{}

// BuildQuery implements [batch_compute.Table].
func (i InvHistoryLog) BuildQuery(graph *batch_compute.GraphContext) string {
	return `
	select '1'
	`
}

// TableName implements [batch_compute.Table].
func (i InvHistoryLog) TableName() string {
	return "inv_history_log"
}

// Temporary implements [batch_compute.Table].
func (i InvHistoryLog) Temporary() bool {
	return true
}
