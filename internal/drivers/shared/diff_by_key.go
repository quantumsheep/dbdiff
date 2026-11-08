package driversshared

import (
	"context"
	"database/sql"
)

type DiffRules[T any] struct {
	Key    func(object T) string
	Create func(desired T) []Instruction
	Change func(desired T, current T) ([]Instruction, error)
	Drop   func(current T) []Instruction
}

func DiffByKey[T any](
	desiredObjects []T,
	currentObjects []T,
	rules DiffRules[T],
) ([]Instruction, []Instruction, error) {
	var additions []Instruction
	var removals []Instruction

	currentByKey := FirstByKey(currentObjects, rules.Key)
	desiredByKey := FirstByKey(desiredObjects, rules.Key)

	for _, desiredObject := range desiredObjects {
		currentObject, found := currentByKey[rules.Key(desiredObject)]
		if !found {
			additions = append(additions, rules.Create(desiredObject)...)
			continue
		}

		changes, err := rules.Change(desiredObject, currentObject)
		if err != nil {
			return nil, nil, err
		}

		additions = append(additions, changes...)
	}

	for _, currentObject := range currentObjects {
		_, found := desiredByKey[rules.Key(currentObject)]
		if !found {
			removals = append(removals, rules.Drop(currentObject)...)
		}
	}

	return additions, removals, nil
}

// Two objects can share a key, and the first one wins, as with a search of the slice.
func FirstByKey[T any](objects []T, key func(T) string) map[string]T {
	objectsByKey := make(map[string]T, len(objects))

	for _, object := range objects {
		objectKey := key(object)

		_, found := objectsByKey[objectKey]
		if !found {
			objectsByKey[objectKey] = object
		}
	}

	return objectsByKey
}

func DiffSides[T any](
	ctx context.Context,
	desiredDatabase *sql.DB,
	currentDatabase *sql.DB,
	get func(context.Context, *sql.DB) ([]T, error),
	rules DiffRules[T],
) ([]Instruction, []Instruction, error) {
	desiredObjects, err := get(ctx, desiredDatabase)
	if err != nil {
		return nil, nil, err
	}

	currentObjects, err := get(ctx, currentDatabase)
	if err != nil {
		return nil, nil, err
	}

	return DiffByKey(desiredObjects, currentObjects, rules)
}

// The rules drop a changed object and create it again. The DROP statement and the CREATE
// statement stay together in the additions, so the drop runs before the create.
func ReplaceRules[T any](
	key func(T) string,
	equal func(T, T) bool,
	create func(T) Instruction,
	drop func(T) Instruction,
) DiffRules[T] {
	return DiffRules[T]{
		Key: key,
		Create: func(desired T) []Instruction {
			return []Instruction{create(desired)}
		},
		Change: func(desired T, current T) ([]Instruction, error) {
			if equal(desired, current) {
				return nil, nil
			}

			return []Instruction{drop(current), create(desired)}, nil
		},
		Drop: func(current T) []Instruction {
			return []Instruction{drop(current)}
		},
	}
}

// The drop of a changed object goes into the removals, in the order of the
// desired objects and before the drops of the removed objects. The caller can then run
// every drop before every create.
func ReplaceByKey[T any](
	desiredObjects []T,
	currentObjects []T,
	key func(T) string,
	equal func(T, T) bool,
	create func(T) Instruction,
	drop func(T) Instruction,
) ([]Instruction, []Instruction) {
	var changedDrops []Instruction

	rules := ReplaceRules(key, equal, create, drop)
	rules.Change = func(desired T, current T) ([]Instruction, error) {
		if equal(desired, current) {
			return nil, nil
		}

		changedDrops = append(changedDrops, drop(current))

		return []Instruction{create(desired)}, nil
	}

	additions, removals, _ := DiffByKey(desiredObjects, currentObjects, rules)

	return additions, append(changedDrops, removals...)
}
