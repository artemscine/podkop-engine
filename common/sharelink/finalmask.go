package sharelink

import (
	"strings"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
)

type xrayMaskEntry struct {
	Type     string          `json:"type"`
	Settings json.RawMessage `json:"settings"`
}

// Recheck after sb.* aliases have been applied: they can change TLS, the
// transport, or the mask itself after the ordinary link parameters were read.
func consistentFinalMask(outboundType string, options *object) error {
	for _, path := range []string{"tls", "transport.download.tls"} {
		value, exists := options.at(path)
		node, ok := value.(*object)
		if !exists || !ok {
			continue
		}
		if _, exists := node.values["finalmask"]; !exists {
			continue
		}
		content, err := node.MarshalJSON()
		if err != nil {
			return refuse("finalmask", "invalid finalmask TLS options")
		}
		var tlsOptions option.OutboundTLSOptions
		if err := json.Unmarshal(content, &tlsOptions); err != nil {
			return refuse("finalmask", "invalid finalmask TLS options")
		}
		if tlsOptions.FinalMask == nil || len(tlsOptions.FinalMask.TCP) == 0 {
			continue
		}
		if err := tlsOptions.FinalMask.Validate(); err != nil {
			return refuse("finalmask", "invalid finalmask fragment settings")
		}
		if !tlsOptions.Enabled || (tlsOptions.Engine != "" && tlsOptions.Engine != "go") || tlsOptions.Fragment || tlsOptions.RecordFragment {
			return refuse("finalmask", "finalmask requires enabled Go TLS/REALITY without native fragment flags")
		}
		transport, _ := options.at("transport.type")
		if outboundType == C.TypeTUIC || outboundType == C.TypeHysteria || outboundType == C.TypeHysteria2 || transport == C.V2RayTransportTypeQUIC {
			return refuse("finalmask", "TCP finalmask cannot be applied to QUIC")
		}
		if transport == C.V2RayTransportTypeXHTTP && len(tlsOptions.ALPN) == 1 && tlsOptions.ALPN[0] == "h3" && (tlsOptions.Reality == nil || !tlsOptions.Reality.Enabled) {
			return refuse("finalmask", "TCP finalmask cannot be applied to XHTTP over HTTP/3")
		}
	}
	return nil
}

type xrayFinalMask struct {
	TCP        []xrayMaskEntry `json:"tcp"`
	UDP        []xrayMaskEntry `json:"udp"`
	QuicParams json.RawMessage `json:"quicParams"`
}

// applyFinalMask preserves supported masks rather than approximating them with
// tls.fragment, whose split positions and timing have different meanings.
func (d *decoder) applyFinalMask(name string, content []byte, tlsOptions *option.OutboundTLSOptions) error {
	if len(content) == 0 || strings.TrimSpace(string(content)) == "null" {
		return nil
	}
	var wire xrayFinalMask
	if _, err := d.readObject(name, content, &wire); err != nil {
		d.warn(name, " (finalmask) is not supported and cannot be read, the connection may not work")
		return nil
	}
	mask := new(option.FinalMaskOptions)
	ignored := false
	for _, entry := range wire.TCP {
		switch strings.ToLower(entry.Type) {
		case "fragment":
			var settings option.FinalMaskFragmentOptions
			if len(entry.Settings) > 0 {
				if _, err := d.readObject(name+".fragment", entry.Settings, &settings); err != nil {
					return refuse("finalmask", name, ": invalid fragment settings: ", plainJSONError(err))
				}
			}
			mask.TCP = append(mask.TCP, option.FinalMaskTCPOptions{Type: "fragment", Settings: settings})
		case "noise":
			ignored = true
		default:
			return refuse("finalmask", name, ": unsupported TCP mask", shown(entry.Type))
		}
	}
	for _, entry := range wire.UDP {
		if strings.ToLower(entry.Type) != "noise" {
			return refuse("finalmask", name, ": unsupported UDP mask", shown(entry.Type))
		}
		ignored = true
	}
	if ignored {
		if len(mask.TCP) == 0 {
			d.warn(name, " (finalmask) is not supported and is ignored: what it sets is the client's own")
		} else {
			d.warn(name, ": noise is not supported and is ignored")
		}
	}
	if raw := strings.TrimSpace(string(wire.QuicParams)); raw != "" && raw != "null" && raw != "{}" {
		d.warn(name, ": quicParams are not supported and are ignored")
	}
	if len(mask.TCP) == 0 {
		return nil
	}
	if err := mask.Validate(); err != nil {
		return refuse("finalmask", name, ": invalid fragment settings")
	}
	if tlsOptions == nil || !tlsOptions.Enabled {
		return refuse("finalmask", name, ": TCP fragment requires TLS or REALITY")
	}
	tlsOptions.FinalMask = mask
	return nil
}
