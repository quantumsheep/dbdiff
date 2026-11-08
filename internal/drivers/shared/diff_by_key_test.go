package driversshared

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiffByKey(t *testing.T) {
	rules := DiffRules[string]{
		Key: func(object string) string {
			return object[:1]
		},
		Create: func(desired string) []Instruction {
			return []Instruction{&SQLCommentInstruction{
				Text: "create " + desired,
			}}
		},
		Change: func(desired string, current string) ([]Instruction, error) {
			if desired == current {
				return nil, nil
			}

			return []Instruction{&SQLCommentInstruction{
				Text: "change " + desired,
			}}, nil
		},
		Drop: func(current string) []Instruction {
			return []Instruction{&SQLCommentInstruction{
				Text: "drop " + current,
			}}
		},
	}

	t.Run("CreateChangeAndDrop", func(t *testing.T) {
		additions, removals, err := DiffByKey([]string{"a1", "b2", "c3"}, []string{"b9", "c3", "d4"}, rules)
		require.NoError(t, err)
		require.Equal(t, []Instruction{
			&SQLCommentInstruction{
				Text: "create a1",
			},
			&SQLCommentInstruction{
				Text: "change b2",
			},
		}, additions)
		require.Equal(t, []Instruction{
			&SQLCommentInstruction{
				Text: "drop d4",
			},
		}, removals)
	})

	t.Run("TwoCurrentObjectsWithOneKey", func(t *testing.T) {
		additions, removals, err := DiffByKey([]string{"b2"}, []string{"b2", "b9"}, rules)
		require.NoError(t, err)
		require.Empty(t, additions)
		require.Empty(t, removals)
	})

	t.Run("EmptySides", func(t *testing.T) {
		additions, removals, err := DiffByKey(nil, nil, rules)
		require.NoError(t, err)
		require.Nil(t, additions)
		require.Nil(t, removals)
	})

	t.Run("ChangeError", func(t *testing.T) {
		failingRules := rules
		failingRules.Change = func(desired string, current string) ([]Instruction, error) {
			return nil, errors.New("change failed")
		}

		additions, removals, err := DiffByKey([]string{"a1"}, []string{"a9"}, failingRules)
		require.EqualError(t, err, "change failed")
		require.Nil(t, additions)
		require.Nil(t, removals)
	})

	desiredDatabase := &sql.DB{}
	currentDatabase := &sql.DB{}

	t.Run("DiffSides", func(t *testing.T) {
		get := func(_ context.Context, db *sql.DB) ([]string, error) {
			if db == desiredDatabase {
				return []string{"a1", "b2"}, nil
			}

			return []string{"b2", "c3"}, nil
		}

		additions, removals, err := DiffSides(t.Context(), desiredDatabase, currentDatabase, get, rules)
		require.NoError(t, err)
		require.Equal(t, []Instruction{
			&SQLCommentInstruction{
				Text: "create a1",
			},
		}, additions)
		require.Equal(t, []Instruction{
			&SQLCommentInstruction{
				Text: "drop c3",
			},
		}, removals)
	})

	t.Run("DiffSidesGetError", func(t *testing.T) {
		get := func(_ context.Context, db *sql.DB) ([]string, error) {
			if db == currentDatabase {
				return nil, errors.New("get failed")
			}

			return []string{"a1"}, nil
		}

		additions, removals, err := DiffSides(t.Context(), desiredDatabase, currentDatabase, get, rules)
		require.EqualError(t, err, "get failed")
		require.Nil(t, additions)
		require.Nil(t, removals)
	})

	replaceKey := func(object string) string {
		return object[:1]
	}
	replaceEqual := func(desired string, current string) bool {
		return desired == current
	}
	replaceCreate := func(desired string) Instruction {
		return &SQLCommentInstruction{
			Text: "create " + desired,
		}
	}
	replaceDrop := func(current string) Instruction {
		return &SQLCommentInstruction{
			Text: "drop " + current,
		}
	}

	t.Run("ReplaceRules", func(t *testing.T) {
		additions, removals, err := DiffByKey([]string{"a1", "b2", "c3"}, []string{"b9", "c3", "d4"},
			ReplaceRules(replaceKey, replaceEqual, replaceCreate, replaceDrop))
		require.NoError(t, err)
		require.Equal(t, []Instruction{
			&SQLCommentInstruction{
				Text: "create a1",
			},
			&SQLCommentInstruction{
				Text: "drop b9",
			},
			&SQLCommentInstruction{
				Text: "create b2",
			},
		}, additions)
		require.Equal(t, []Instruction{
			&SQLCommentInstruction{
				Text: "drop d4",
			},
		}, removals)
	})

	t.Run("ReplaceByKey", func(t *testing.T) {
		additions, removals := ReplaceByKey([]string{"a1", "e5", "b2", "c3"}, []string{"d4", "b9", "c3", "e8"},
			replaceKey, replaceEqual, replaceCreate, replaceDrop)
		require.Equal(t, []Instruction{
			&SQLCommentInstruction{
				Text: "create a1",
			},
			&SQLCommentInstruction{
				Text: "create e5",
			},
			&SQLCommentInstruction{
				Text: "create b2",
			},
		}, additions)
		require.Equal(t, []Instruction{
			&SQLCommentInstruction{
				Text: "drop e8",
			},
			&SQLCommentInstruction{
				Text: "drop b9",
			},
			&SQLCommentInstruction{
				Text: "drop d4",
			},
		}, removals)
	})
}
