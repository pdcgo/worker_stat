package metric_key

import (
	"fmt"
	"path"
	"time"
)

type Prefix string

func (p Prefix) Join(s string) string {
	return path.Join(string(p), s)
}

func NewDailyTeamPrefix(t time.Time, teamID uint64) Prefix {
	return Prefix(fmt.Sprintf("daily/%s/team/%d", t.Format("2006-01-02"), teamID))
}

func NewErrorPrefix(p Prefix) Prefix {
	return Prefix(fmt.Sprintf("error/%s", p))
}
