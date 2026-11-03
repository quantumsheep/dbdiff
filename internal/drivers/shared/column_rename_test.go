package driversshared

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testColumn struct {
	Name string
	Type string
}

func TestGuessRenamedColumns(t *testing.T) {
	name := func(column testColumn) string {
		return column.Name
	}
	equalExceptName := func(first testColumn, second testColumn) bool {
		return first.Type == second.Type
	}

	t.Run("OneCandidate", func(t *testing.T) {
		renamedFrom := GuessRenamedColumns(
			[]testColumn{
				{
					Name: "id",
					Type: "INTEGER",
				},
				{
					Name: "full_name",
					Type: "TEXT",
				},
			},
			[]testColumn{
				{
					Name: "id",
					Type: "INTEGER",
				},
				{
					Name: "name",
					Type: "TEXT",
				},
			},
			name, equalExceptName)
		require.Equal(t, map[string]string{
			"full_name": "name",
		}, renamedFrom)
	})

	t.Run("TwoCandidates", func(t *testing.T) {
		renamedFrom := GuessRenamedColumns(
			[]testColumn{
				{
					Name: "full_name",
					Type: "TEXT",
				},
			},
			[]testColumn{
				{
					Name: "first_name",
					Type: "TEXT",
				},
				{
					Name: "last_name",
					Type: "TEXT",
				},
			},
			name, equalExceptName)
		require.Equal(t, map[string]string{}, renamedFrom)
	})

	t.Run("CandidateUsedOnce", func(t *testing.T) {
		renamedFrom := GuessRenamedColumns(
			[]testColumn{
				{
					Name: "first",
					Type: "TEXT",
				},
				{
					Name: "second",
					Type: "TEXT",
				},
			},
			[]testColumn{
				{
					Name: "old",
					Type: "TEXT",
				},
			},
			name, equalExceptName)
		require.Equal(t, map[string]string{
			"first": "old",
		}, renamedFrom)
	})

	t.Run("DifferentAttributes", func(t *testing.T) {
		renamedFrom := GuessRenamedColumns(
			[]testColumn{
				{
					Name: "new",
					Type: "TEXT",
				},
			},
			[]testColumn{
				{
					Name: "old",
					Type: "INTEGER",
				},
			},
			name, equalExceptName)
		require.Equal(t, map[string]string{}, renamedFrom)
	})
}
