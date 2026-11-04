package driversshared

import (
	"slices"

	"github.com/samber/lo"
)

// The map goes from the new name to the old name. A desired column that the current
// schema does not hold is a rename only if exactly one current column that the desired
// schema does not hold is equal to it except for the name. Two candidates make the
// guess unsafe, so the caller adds the new column and removes the old columns.
func GuessRenamedColumns[Column any](
	desiredColumns []Column,
	currentColumns []Column,
	name func(Column) string,
	equalExceptName func(Column, Column) bool,
) map[string]string {
	desiredNames := lo.Map(desiredColumns, func(column Column, _ int) string {
		return name(column)
	})

	currentNames := lo.Map(currentColumns, func(column Column, _ int) string {
		return name(column)
	})

	newNameToOldName := make(map[string]string)

	for _, desiredColumn := range desiredColumns {
		if slices.Contains(currentNames, name(desiredColumn)) {
			continue
		}

		candidates := lo.Filter(currentColumns, func(currentColumn Column, _ int) bool {
			_, alreadyRenamed := lo.FindKey(newNameToOldName, name(currentColumn))
			return !slices.Contains(desiredNames, name(currentColumn)) && !alreadyRenamed &&
				equalExceptName(currentColumn, desiredColumn)
		})
		if len(candidates) == 1 {
			newNameToOldName[name(desiredColumn)] = name(candidates[0])
		}
	}

	return newNameToOldName
}
