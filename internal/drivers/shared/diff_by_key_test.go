package driversshared

import (
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
}
