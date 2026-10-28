package option

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// FinalMaskOptions describes client-side TCP fragmentation in Xray's FinalMask
// format. It is deliberately separate from fragment/record_fragment, whose
// segmentation is based on the server name rather than byte ranges.
type FinalMaskOptions struct {
	TCP []FinalMaskTCPOptions `json:"tcp,omitempty"`
}

type FinalMaskTCPOptions struct {
	Type     string                   `json:"type"`
	Settings FinalMaskFragmentOptions `json:"settings"`
}

type FinalMaskFragmentOptions struct {
	Packets  string            `json:"packets,omitempty"`
	Length   *V2RayXHTTPRange  `json:"length,omitempty"`
	Delay    *V2RayXHTTPRange  `json:"delay,omitempty"`
	Lengths  []V2RayXHTTPRange `json:"lengths,omitempty"`
	Delays   []V2RayXHTTPRange `json:"delays,omitempty"`
	MaxSplit *V2RayXHTTPRange  `json:"maxSplit,omitempty"`
}

// PacketRange returns the inclusive Write indices, or (0, 1) for tlshello.
// An empty packets value selects every Write, as it does in Xray.
func (o FinalMaskFragmentOptions) PacketRange() (uint64, uint64, error) {
	switch strings.ToLower(o.Packets) {
	case "":
		return 0, 0, nil
	case "tlshello":
		return 0, 1, nil
	default:
		var r V2RayXHTTPRange
		if err := json.Unmarshal([]byte(strconv.Quote(o.Packets)), &r); err != nil || r.From <= 0 {
			return 0, 0, fmt.Errorf("finalmask fragment: packets must be tlshello or positive Write indices")
		}
		return uint64(r.From), uint64(r.To), nil
	}
}

func (o *FinalMaskOptions) Validate() error {
	if o == nil {
		return nil
	}
	for i, mask := range o.TCP {
		if strings.ToLower(mask.Type) != "fragment" {
			return fmt.Errorf("finalmask tcp[%d]: only fragment is supported", i)
		}
		if err := mask.Settings.Validate(); err != nil {
			return fmt.Errorf("finalmask tcp[%d]: %w", i, err)
		}
	}
	return nil
}

func (o FinalMaskFragmentOptions) Validate() error {
	if _, _, err := o.PacketRange(); err != nil {
		return err
	}
	lengths := o.Lengths
	if len(lengths) == 0 {
		if o.Length == nil {
			return fmt.Errorf("fragment length is required")
		}
		lengths = []V2RayXHTTPRange{*o.Length}
	}
	for _, r := range lengths {
		if r.From < 0 || r.To < r.From {
			return fmt.Errorf("fragment lengths must be nonnegative ordered ranges")
		}
	}
	if lengths[len(lengths)-1].From == 0 {
		return fmt.Errorf("fragment last length must be positive")
	}
	delays := o.Delays
	if len(delays) == 0 && o.Delay != nil {
		delays = []V2RayXHTTPRange{*o.Delay}
	}
	for _, r := range delays {
		if r.From < 0 || r.To < r.From {
			return fmt.Errorf("fragment delays must be nonnegative ordered ranges")
		}
	}
	if o.MaxSplit != nil && (o.MaxSplit.From < 0 || o.MaxSplit.To < o.MaxSplit.From) {
		return fmt.Errorf("fragment maxSplit must be a nonnegative ordered range")
	}
	return nil
}
