package drivers

import (
	"context"
	"strings"
)

type Driver interface {
	Close() error
	Diff(ctx context.Context) (string, error)
}

// A SectionDiff holds the statements of one kind of schema object. A driver prints the
// early removals in the reverse section order, then the additions in the section order,
// then the removals in the reverse section order. An object that uses another object must
// go away first.
//
// An early removal goes away before every addition. An addition of one section can need
// that removal. A view of the view section reads a column of the table section, and that
// view blocks a change of the column.
type SectionDiff struct {
	EarlyRemovals string
	Additions     string
	Removals      string
}

func newSectionDiff(additions *strings.Builder, removals *strings.Builder) *SectionDiff {
	sectionDiff := &SectionDiff{
		Additions: strings.TrimSpace(additions.String()),
		Removals:  strings.TrimSpace(removals.String()),
	}

	return sectionDiff
}
