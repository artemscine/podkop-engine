package cli

import (
	"github.com/sagernet/sing-box/log"

	"github.com/spf13/cobra"
)

// Main runs the command line of sing-box, as the program cmd/sing-box does.
func Main() {
	if err := mainCommand.Execute(); err != nil {
		log.Fatal(err)
	}
}

// Command is the root command of sing-box, for a program that runs sing-box under a name
// of its own beside commands of its own.
func Command() *cobra.Command {
	return mainCommand
}
