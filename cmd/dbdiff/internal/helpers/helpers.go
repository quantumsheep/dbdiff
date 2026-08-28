package helpers

import (
	"context"
	"runtime/debug"

	"github.com/urfave/cli/v3"
)

// The release workflow writes the tag into this variable with the linker flag -X.
var version = ""

func Version() string {
	if version != "" {
		return version
	}

	buildInfo, found := debug.ReadBuildInfo()
	if found && buildInfo.Main.Version != "" && buildInfo.Main.Version != "(devel)" {
		return buildInfo.Main.Version
	}

	return "unknown"
}

func OnUsageError(ctx context.Context, command *cli.Command, err error, isSubcommand bool) error {
	return err
}
