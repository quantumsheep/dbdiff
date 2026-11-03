package driversshared

import (
	"slices"

	"github.com/samber/lo"
)

// The map goes from the new name to the old name. A target column
// that the source does not hold is a rename only if exactly one source column that the
// target does not hold is equal to it except for the name. Two candidates make the guess
// unsafe, so the caller adds the new column and removes the old columns.
func GuessRenamedColumns[Column any](targetColumns []Column, sourceColumns []Column, name func(Column) string, equalExceptName func(Column, Column) bool) map[string]string {
	targetNames := lo.Map(targetColumns, func(column Column, _ int) string {
		return name(column)
	})

	sourceNames := lo.Map(sourceColumns, func(column Column, _ int) string {
		return name(column)
	})

	newNameToOldName := make(map[string]string)

	for _, targetColumn := range targetColumns {
		if slices.Contains(sourceNames, name(targetColumn)) {
			continue
		}

		candidates := lo.Filter(sourceColumns, func(sourceColumn Column, _ int) bool {
			_, alreadyRenamed := lo.FindKey(newNameToOldName, name(sourceColumn))
			return !slices.Contains(targetNames, name(sourceColumn)) && !alreadyRenamed &&
				equalExceptName(sourceColumn, targetColumn)
		})
		if len(candidates) == 1 {
			newNameToOldName[name(targetColumn)] = name(candidates[0])
		}
	}

	return newNameToOldName
}
