// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Command awrel is the awrel command-line tool.
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"go.tbhb.dev/awrel/internal/buildmeta"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, os.Getenv))
}

// run executes the root command and returns the process exit code. It reads
// the environment only through the lookup function passed in.
func run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string) int {
	cmd := newRootCmd(stdin, getenv)
	cmd.SetArgs(args)
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)

	err := cmd.ExecuteContext(context.Background())
	if err != nil {
		return 1
	}

	return 0
}

func versionString() string {
	info := buildmeta.Get()

	return fmt.Sprintf("%s (commit %s, date %s)", info.Version, info.Commit, info.Date)
}

func newRootCmd(stdin io.Reader, getenv func(string) string) *cobra.Command {
	root := &cobra.Command{
		Use:          "awrel",
		Short:        "awrel command-line tool",
		Version:      versionString(),
		SilenceUsage: true,
	}
	root.SetVersionTemplate("awrel {{.Version}}\n")
	root.AddCommand(newVersionCmd(), newMCPCmd(stdin, getenv))

	return root
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version, commit and date",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			fmt.Fprintf(cmd.OutOrStdout(), "awrel %s\n", versionString())
		},
	}
}
