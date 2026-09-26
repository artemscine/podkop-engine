//go:build podkop_slim

package include

import (
	"context"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/adapter/service"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

func registerSnellInbound(registry *inbound.Registry) {
	inbound.Register[option.SnellInboundOptions](registry, C.TypeSnell, func(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.SnellInboundOptions) (adapter.Inbound, error) {
		return nil, E.New(`snell is not included in this build (podkop_slim)`)
	})
}

func registerSnellOutbound(registry *outbound.Registry) {
	outbound.Register[option.SnellOutboundOptions](registry, C.TypeSnell, func(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.SnellOutboundOptions) (adapter.Outbound, error) {
		return nil, E.New(`snell is not included in this build (podkop_slim)`)
	})
}

func registerAPIService(registry *service.Registry) {
	service.Register[option.APIServiceOptions](registry, C.TypeAPI, func(ctx context.Context, logger log.ContextLogger, tag string, options option.APIServiceOptions) (adapter.Service, error) {
		return nil, E.New(`API service is not included in this build (podkop_slim)`)
	})
}
