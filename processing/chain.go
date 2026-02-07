package processing

import "context"

type ChainNextFunc[T any] func(ctx context.Context, data T) error
type ChainNextHandler[T any] func(next ChainNextFunc[T]) ChainNextFunc[T]

func NewChain[T any](chains ...ChainNextHandler[T]) ChainNextFunc[T] {

	var next ChainNextFunc[T] = func(ctx context.Context, data T) error {
		return nil
	}

	reverse(chains)
	for _, chain := range chains {
		next = chain(next)
	}

	return next
}

func reverse[T any](s []T) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}
