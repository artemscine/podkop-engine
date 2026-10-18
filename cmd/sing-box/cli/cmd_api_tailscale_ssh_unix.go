//go:build !podkop_slim && !windows

package cli

import (
	"os"
	"syscall"
)

func executeSSH(path string, argv []string) error {
	return syscall.Exec(path, argv, os.Environ())
}
