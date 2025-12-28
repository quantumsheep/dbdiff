package drivers

import "context"

type Driver interface {
	Close() error
	Diff(ctx context.Context) (string, error)
}
