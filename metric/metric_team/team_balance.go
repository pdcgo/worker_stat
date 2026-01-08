package metric_team

//go:generate metric_generate
type TeamLastBalance struct {
	ID      int64   `metric:"id" json:"id" gorm:"primaryKey;autoIncrement:false"`
	TeamID  uint64  `metric:"index"`
	Ts      int64   `json:"ts"`
	Balance float64 `json:"balance"`
}
