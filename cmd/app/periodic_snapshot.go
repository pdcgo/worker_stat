package main

import (
	"context"
	"log"
	"log/slog"
	"time"

	"github.com/wargasipil/stream_engine/stream_core"
)

type PeriodicSnapshot func(ctx context.Context, dur time.Duration)

func NewPeriodicSnapshot(kv stream_core.KeyStore) PeriodicSnapshot {

	return func(ctx context.Context, dur time.Duration) {
		var err error
		last := time.Now()
		ti := time.NewTimer(dur)
		for {

			select {
			case <-ctx.Done():
				return
			case <-ti.C:
				err = kv.UpdatedKey(last, func(key string) error {
					log.Println(key)
					return nil
				})
				if err != nil {
					slog.Error(err.Error())
				}
				last = time.Now()
				ti.Reset(dur)
			}

		}
	}
}
