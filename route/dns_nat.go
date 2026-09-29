package route

import (
	"context"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	R "github.com/sagernet/sing-box/route/rule"
	"github.com/sagernet/sing/common/buf"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/udpnat2"

	mDNS "github.com/miekg/dns"
)

// DNS hijacked from a UDP session of an inbound's NAT table (direct, mixed, socks) is
// answered from a packet handler of the session, as sing-box 1.13 did, not from a
// reading loop per session. dnsmasq forwards every query from a new port, so every query
// is a new session, and the loop kept three goroutines, a timer and buffers alive for
// 10 s after each answer.
func (r *Router) hijackDNSNatConn(ctx context.Context, conn udpnat.Conn, packetBuffers []*N.PacketBuffer, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	metadata.Destination = M.Socksaddr{}
	handler := &dnsNatHandler{router: r, ctx: ctx, conn: conn, metadata: metadata, onClose: onClose}
	for _, packet := range packetBuffers {
		handler.NewPacketEx(packet.Buffer, packet.Destination)
		N.PutPacketBuffer(packet)
	}
	// the session leaves the table 10 s after its last packet, not after the UDP timeout
	conn.SetTimeout(C.DNSTimeout)
	conn.SetHandler(handler)
}

type dnsNatHandler struct {
	router   *Router
	ctx      context.Context
	conn     udpnat.Conn
	metadata adapter.InboundContext
	onClose  N.CloseHandlerFunc
}

func (h *dnsNatHandler) NewPacketEx(buffer *buf.Buffer, destination M.Socksaddr) {
	var message mDNS.Msg
	err := message.Unpack(buffer.Bytes())
	buffer.Release()
	if err != nil {
		h.router.logger.ErrorContext(h.ctx, E.Cause(err, "process DNS packet: unpack request"))
		return
	}
	metadata := h.metadata
	h.router.dns.ExchangeAsync(adapter.WithContext(h.ctx, &metadata), &message, adapter.DNSQueryOptions{}, func(response *mDNS.Msg, exchangeErr error) {
		if exchangeErr == nil {
			exchangeErr = h.router.writeDNSPacketResponse(&message, response, h.conn, destination)
		}
		if exchangeErr != nil && !R.IsRejected(exchangeErr) && !E.IsClosedOrCanceled(exchangeErr) {
			h.router.logger.ErrorContext(h.ctx, E.Cause(exchangeErr, "process DNS packet"))
		}
	})
}

// Close is called when the session leaves the NAT table.
func (h *dnsNatHandler) Close() error {
	if h.onClose != nil {
		h.onClose(nil)
	}
	return nil
}
