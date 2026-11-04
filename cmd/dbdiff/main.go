package main

import (
	"context"
	"os"

	cmdroot "github.com/quantumsheep/dbdiff/cmd/dbdiff/cmd/root"
)

func main() {
	err := cmdroot.Command().Run(context.Background(), os.Args)

	os.Exit(cmdroot.ExitCode(err, os.Stderr))
}
