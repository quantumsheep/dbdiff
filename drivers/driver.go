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
// early removals first, then the additions, then the removals. An early removal covers the
// object that blocks an addition of another section.
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

// newSectionDiffFromInstructions renders two instruction lists into a SectionDiff. The
// helper exists while some sections build instructions and others build text.
func newSectionDiffFromInstructions(additions []Instruction, removals []Instruction) *SectionDiff {
	return &SectionDiff{
		Additions: RenderInstructions(additions),
		Removals:  RenderInstructions(removals),
	}
}
