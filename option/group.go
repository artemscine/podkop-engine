package option

import "github.com/sagernet/sing/common/json/badoption"

type SelectorOutboundOptions struct {
	Outbounds                 []string `json:"outbounds" reference:"outbound"`
	Default                   string   `json:"default,omitempty" reference:"outbound"`
	InterruptExistConnections bool     `json:"interrupt_exist_connections,omitempty"`
}

type URLTestOutboundOptions struct {
	FallbackSelection         string                          `json:"fallback_selection,omitempty"`
	FallbackRecovery          *URLTestFallbackRecoveryOptions `json:"fallback_recovery,omitempty"`
	Outbounds                 []string                        `json:"outbounds" reference:"outbound"`
	Fallbacks                 []string                        `json:"fallbacks,omitempty" reference:"outbound"`
	URL                       string                          `json:"url,omitempty"`
	DownloadURL               *string                         `json:"download_url,omitempty"`
	Interval                  badoption.Duration              `json:"interval,omitempty"`
	Tolerance                 uint16                          `json:"tolerance,omitempty"`
	IdleTimeout               badoption.Duration              `json:"idle_timeout,omitempty"`
	InterruptExistConnections bool                            `json:"interrupt_exist_connections,omitempty"`
}

type URLTestFallbackRecoveryOptions struct {
	Successes      int                 `json:"successes,omitempty"`
	MinHealthyTime *badoption.Duration `json:"min_healthy_time,omitempty"`
}
