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
