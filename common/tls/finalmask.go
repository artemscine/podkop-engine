package tls

import (
	"github.com/sagernet/sing-box/common/finalmask"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

func parseFinalMaskOptions(options option.OutboundTLSOptions) (*finalmask.Config, error) {
	if options.FinalMask == nil || len(options.FinalMask.TCP) == 0 {
		return nil, nil
	}
	if !options.Enabled {
		return nil, E.New("finalmask requires enabled TLS or REALITY")
	}
	if options.Fragment || options.RecordFragment {
		return nil, E.New("finalmask cannot be combined with fragment or record_fragment")
	}
	if options.KernelTx || options.KernelRx {
		return nil, E.New("finalmask cannot be combined with kTLS")
	}
	return finalmask.New(options.FinalMask)
}
