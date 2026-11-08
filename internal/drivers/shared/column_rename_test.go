package driversshared

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type testColumn struct {
	Name string
	Type string
}

func TestRenamedColumns(t *testing.T) {
	name := func(column testColumn) string {
		return column.Name
	}
	equalExceptName := func(first testColumn, second testColumn) bool {
		return first.Type == second.Type
	}

	t.Run("OneCandidate", func(t *testing.T) {
		renamedFrom := RenamedColumns(
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
			name, strings.ToLower, equalExceptName, true)
		require.Equal(t, map[string]string{
			"full_name": "name",
		}, renamedFrom)
	})

	t.Run("TwoCandidates", func(t *testing.T) {
		renamedFrom := RenamedColumns(
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
			name, strings.ToLower, equalExceptName, true)
		require.Equal(t, map[string]string{}, renamedFrom)
	})

	t.Run("CandidateUsedOnce", func(t *testing.T) {
		renamedFrom := RenamedColumns(
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
			name, strings.ToLower, equalExceptName, true)
		require.Equal(t, map[string]string{
			"first": "old",
		}, renamedFrom)
	})

	t.Run("DifferentAttributes", func(t *testing.T) {
		renamedFrom := RenamedColumns(
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
			name, strings.ToLower, equalExceptName, true)
		require.Equal(t, map[string]string{}, renamedFrom)
	})

	t.Run("NameInOtherCase", func(t *testing.T) {
		renamedFrom := RenamedColumns(
			[]testColumn{
				{
					Name: "email",
					Type: "TEXT",
				},
			},
			[]testColumn{
				{
					Name: "Email",
					Type: "INTEGER",
				},
			},
			name, strings.ToLower, equalExceptName, false)
		require.Equal(t, map[string]string{
			"email": "Email",
		}, renamedFrom)
	})

	t.Run("GuessSkipsANameInOtherCase", func(t *testing.T) {
		renamedFrom := RenamedColumns(
			[]testColumn{
				{
					Name: "email",
					Type: "TEXT",
				},
				{
					Name: "contact",
					Type: "TEXT",
				},
			},
			[]testColumn{
				{
					Name: "Email",
					Type: "TEXT",
				},
			},
			name, strings.ToLower, equalExceptName, true)
		require.Equal(t, map[string]string{
			"email": "Email",
		}, renamedFrom)
	})

	t.Run("NoGuess", func(t *testing.T) {
		renamedFrom := RenamedColumns(
			[]testColumn{
				{
					Name: "full_name",
					Type: "TEXT",
				},
			},
			[]testColumn{
				{
					Name: "name",
					Type: "TEXT",
				},
			},
			name, strings.ToLower, equalExceptName, false)
		require.Equal(t, map[string]string{}, renamedFrom)
	})
}
