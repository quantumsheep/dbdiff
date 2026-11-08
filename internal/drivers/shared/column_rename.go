package driversshared

import (
	"github.com/samber/lo"
)

// The map goes from the new name to the old name. The key function gives the name that the
// engine compares, so a desired column with the key of a current column but another name
// is always a rename. Without that rename, the diff drops the column and its values.
//
// A guess adds a desired column that the current schema does not hold as a rename only if
// exactly one current column that the desired schema does not hold is equal to it except
// for the name. Two candidates make the guess unsafe, so the caller adds the new column and
// removes the old columns.
func RenamedColumns[Column any](
	desiredColumns []Column,
	currentColumns []Column,
	name func(Column) string,
	key func(string) string,
	equalExceptName func(Column, Column) bool,
	guess bool,
) map[string]string {
	columnKey := func(column Column) string {
		return key(name(column))
	}

	desiredKeys := lo.SliceToMap(desiredColumns, func(column Column) (string, bool) {
		return columnKey(column), true
	})

	currentByKey := lo.KeyBy(currentColumns, columnKey)

	newNameToOldName := make(map[string]string)

	for _, desiredColumn := range desiredColumns {
		currentColumn, found := currentByKey[columnKey(desiredColumn)]
		if found && name(currentColumn) != name(desiredColumn) {
			newNameToOldName[name(desiredColumn)] = name(currentColumn)
		}
	}

	if !guess {
		return newNameToOldName
	}

	renamedOldNames := make(map[string]bool)

	for _, desiredColumn := range desiredColumns {
		_, found := currentByKey[columnKey(desiredColumn)]
		if found {
			continue
		}

		candidates := lo.Filter(currentColumns, func(currentColumn Column, _ int) bool {
			return !desiredKeys[columnKey(currentColumn)] && !renamedOldNames[name(currentColumn)] &&
				equalExceptName(currentColumn, desiredColumn)
		})
		if len(candidates) == 1 {
			newNameToOldName[name(desiredColumn)] = name(candidates[0])
			renamedOldNames[name(candidates[0])] = true
		}
	}

	return newNameToOldName
}
