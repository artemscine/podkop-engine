//go:build !podkop_slim

package include

import (
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/adapter/service"
	"github.com/sagernet/sing-box/protocol/snell"
	"github.com/sagernet/sing-box/service/api"
)

func registerSnellInbound(registry *inbound.Registry) {
	snell.RegisterInbound(registry)
}

func registerSnellOutbound(registry *outbound.Registry) {
	snell.RegisterOutbound(registry)
}

func registerAPIService(registry *service.Registry) {
	api.RegisterService(registry)
}
