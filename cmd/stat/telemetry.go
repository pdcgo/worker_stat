package main

import (
	"context"

	"github.com/urfave/cli/v3"
	"go.opentelemetry.io/otel"
)

func WithTelemetry(name string, next cli.ActionFunc) cli.ActionFunc {
	return func(ctx context.Context, c *cli.Command) error {
		ctx, span := otel.GetTracerProvider().Tracer("").Start(ctx, name)
		defer span.End()
		return next(ctx, c)
	}
}
