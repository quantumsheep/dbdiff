package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	cmdroot "github.com/quantumsheep/dbdiff/cmd/dbdiff/cmd/root"
)

func main() {
	// The drivers drop their temporary databases and stop their temporary servers when the
	// context stops. Without the signal context, an interrupt ends the process before that
	// cleanup.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	err := cmdroot.Command().Run(ctx, os.Args)

	stop()
	os.Exit(cmdroot.ExitCode(err, os.Stderr))
}
