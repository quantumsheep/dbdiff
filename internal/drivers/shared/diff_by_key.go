package driversshared

import (
	"github.com/samber/lo"
)

type DiffRules[T any] struct {
	Key    func(object T) string
	Create func(target T) []Instruction
	Change func(target T, source T) ([]Instruction, error)
	Drop   func(source T) []Instruction
}

func DiffByKey[T any](targetObjects []T, sourceObjects []T, rules DiffRules[T]) ([]Instruction, []Instruction, error) {
	var additions []Instruction
	var removals []Instruction

	for _, targetObject := range targetObjects {
		sourceObject, found := lo.Find(sourceObjects, func(object T) bool {
			return rules.Key(object) == rules.Key(targetObject)
		})
		if !found {
			additions = append(additions, rules.Create(targetObject)...)
			continue
		}

		changes, err := rules.Change(targetObject, sourceObject)
		if err != nil {
			return nil, nil, err
		}

		additions = append(additions, changes...)
	}

	for _, sourceObject := range sourceObjects {
		_, found := lo.Find(targetObjects, func(object T) bool {
			return rules.Key(object) == rules.Key(sourceObject)
		})
		if !found {
			removals = append(removals, rules.Drop(sourceObject)...)
		}
	}

	return additions, removals, nil
}

// The rules drop a changed object and create it again. The DROP statement and the CREATE
// statement stay together in the additions, so the drop runs before the create.
func ReplaceRules[T any](key func(T) string, equal func(T, T) bool, create func(T) Instruction, drop func(T) Instruction) DiffRules[T] {
	return DiffRules[T]{
		Key: key,
		Create: func(target T) []Instruction {
			return []Instruction{create(target)}
		},
		Change: func(target T, source T) ([]Instruction, error) {
			if equal(target, source) {
				return nil, nil
			}

			return []Instruction{drop(source), create(target)}, nil
		},
		Drop: func(source T) []Instruction {
			return []Instruction{drop(source)}
		},
	}
}

// The drop of a changed object goes into the removals, in the order of the
// target objects and before the drops of the removed objects. The caller can then run
// every drop before every create.
func ReplaceByKey[T any](targetObjects []T, sourceObjects []T, key func(T) string, equal func(T, T) bool, create func(T) Instruction, drop func(T) Instruction) ([]Instruction, []Instruction) {
	var changedDrops []Instruction

	rules := ReplaceRules(key, equal, create, drop)
	rules.Change = func(target T, source T) ([]Instruction, error) {
		if equal(target, source) {
			return nil, nil
		}

		changedDrops = append(changedDrops, drop(source))

		return []Instruction{create(target)}, nil
	}

	additions, removals, _ := DiffByKey(targetObjects, sourceObjects, rules)

	return additions, append(changedDrops, removals...)
}
