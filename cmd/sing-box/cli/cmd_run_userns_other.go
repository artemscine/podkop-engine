//go:build !linux

package cli

import "github.com/sagernet/sing-box/option"

func runInUserNamespaceIfNeeded(options option.Options, optionsList []*OptionsEntry) error {
	return nil
}
