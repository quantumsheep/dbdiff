package drivers

import (
	"context"
)

type Driver interface {
	Close() error
	Diff(ctx context.Context) ([]Instruction, error)
}

// A SectionDiff holds the instructions of one kind of schema object. A driver prints the
// early removals first, then the additions, then the removals. An early removal covers the
// object that blocks an addition of another section.
type SectionDiff struct {
	EarlyRemovals []Instruction
	Additions     []Instruction
	Removals      []Instruction
}
