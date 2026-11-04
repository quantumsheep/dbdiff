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
		Create: func(target string) []Instruction {
			return []Instruction{&SQLCommentInstruction{
				Text: "create " + target,
			}}
		},
		Change: func(target string, source string) ([]Instruction, error) {
			if target == source {
				return nil, nil
			}

			return []Instruction{&SQLCommentInstruction{
				Text: "change " + target,
			}}, nil
		},
		Drop: func(source string) []Instruction {
			return []Instruction{&SQLCommentInstruction{
				Text: "drop " + source,
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

	t.Run("EmptySides", func(t *testing.T) {
		additions, removals, err := DiffByKey(nil, nil, rules)
		require.NoError(t, err)
		require.Nil(t, additions)
		require.Nil(t, removals)
	})

	t.Run("ChangeError", func(t *testing.T) {
		failingRules := rules
		failingRules.Change = func(target string, source string) ([]Instruction, error) {
			return nil, errors.New("change failed")
		}

		additions, removals, err := DiffByKey([]string{"a1"}, []string{"a9"}, failingRules)
		require.EqualError(t, err, "change failed")
		require.Nil(t, additions)
		require.Nil(t, removals)
	})

	targetDatabase := &sql.DB{}
	sourceDatabase := &sql.DB{}

	t.Run("DiffSides", func(t *testing.T) {
		get := func(_ context.Context, db *sql.DB) ([]string, error) {
			if db == targetDatabase {
				return []string{"a1", "b2"}, nil
			}

			return []string{"b2", "c3"}, nil
		}

		additions, removals, err := DiffSides(t.Context(), targetDatabase, sourceDatabase, get, rules)
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
			if db == sourceDatabase {
				return nil, errors.New("get failed")
			}

			return []string{"a1"}, nil
		}

		additions, removals, err := DiffSides(t.Context(), targetDatabase, sourceDatabase, get, rules)
		require.EqualError(t, err, "get failed")
		require.Nil(t, additions)
		require.Nil(t, removals)
	})

	replaceKey := func(object string) string {
		return object[:1]
	}
	replaceEqual := func(target string, source string) bool {
		return target == source
	}
	replaceCreate := func(target string) Instruction {
		return &SQLCommentInstruction{
			Text: "create " + target,
		}
	}
	replaceDrop := func(source string) Instruction {
		return &SQLCommentInstruction{
			Text: "drop " + source,
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
