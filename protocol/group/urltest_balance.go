package group

import (
	"context"
	"hash/fnv"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	N "github.com/sagernet/sing/common/network"
)

// The mode balance (podkop-engine).
//
// The connections of a group are spread over its best members by their source address: a
// device keeps the member it was given, so that a site sees one address of it, and a new
// device is given one by rendezvous hashing over the members of the balance, which moves
// few devices when a member comes or goes. A device moves to another member only when its
// own is down, or, once it is out of the balance, when the device has opened no connection
// for balance_idle. On the stand the balance brought no speed: a device's connections over
// two servers went no faster than over one, and a slow member made its devices slow; what
// it gives is the load of the servers spread, and counts of loss of several of them.
//
// The members of the balance are at most balance_size outbounds that are up, whose delay
// (weighed by loss as in the mode loss) is within the tolerance of the best one, and whose
// loss, less the loss of the line, is not over loss_threshold. When none is, the group
// goes by its plain choice, the fastest.

const (
	defaultBalanceSize = 3
	// defaultBalanceIdle is how long a device opens no connection before it may move: a
	// value to be measured on routers with users.
	defaultBalanceIdle = 5 * time.Minute
	// balanceForget is how long a device that opens no connection is remembered.
	balanceForget = time.Hour
)

type balancer struct {
	access    sync.Mutex
	tcp       []adapter.Outbound // the members of the balance for TCP, the best first
	udp       []adapter.Outbound
	devices   map[string]*balanceDevice
	lastSweep time.Time
}

type balanceDevice struct {
	member adapter.Outbound
	active int
	last   time.Time
}

func newBalancer() *balancer {
	return &balancer{devices: make(map[string]*balanceDevice)}
}

// balanceMembers chooses the members of the balance for the network.
func (g *URLTestGroup) balanceMembers(network string, view lossView) []adapter.Outbound {
	ranked, delays := g.rankedOutbounds(network, view)
	var members []adapter.Outbound
	for i, member := range ranked {
		if len(members) >= g.loss.balanceSize || int(delays[i]) > int(delays[0])+int(g.tolerance) {
			break
		}
		if loss, known := view[RealTag(member, network)]; known && loss.excess > g.loss.threshold {
			continue
		}
		members = append(members, member)
	}
	return members
}

// updateBalance chooses the members of the balance anew, after a selection.
func (g *URLTestGroup) updateBalance() {
	if g.mode != modeBalance {
		return
	}
	view := g.lossView()
	tcp, udp := g.balanceMembers(N.NetworkTCP, view), g.balanceMembers(N.NetworkUDP, view)
	b := g.balance
	b.access.Lock()
	changed := !slices.Equal(tcp, b.tcp)
	b.tcp, b.udp = tcp, udp
	b.access.Unlock()
	if changed {
		tags := make([]string, len(tcp))
		for i, member := range tcp {
			tags[i] = member.Tag()
		}
		g.logger.Info("balance over ", len(tags), " outbounds: ", strings.Join(tags, ", "))
	}
}

// balanceKey is the device a connection comes from: its source address, the same for its TCP
// and its UDP, so that a site sees one address of it over both (HTTPS and QUIC).
func balanceKey(ctx context.Context) string {
	if metadata := adapter.ContextFrom(ctx); metadata != nil && metadata.Source.Addr.IsValid() {
		return metadata.Source.Addr.Unmap().String()
	}
	return ""
}

// rendezvous is the member of the members the key falls on.
func rendezvous(key string, members []adapter.Outbound) adapter.Outbound {
	var (
		best      adapter.Outbound
		bestScore uint64
	)
	for _, member := range members {
		hash := fnv.New64a()
		hash.Write([]byte(key))
		hash.Write([]byte{0})
		hash.Write([]byte(member.Tag()))
		if score := hash.Sum64(); best == nil || score > bestScore {
			best, bestScore = member, score
		}
	}
	return best
}

// balanced chooses the member a connection of the network goes by, and returns what to
// call when the connection is closed; nil when the balance has no member for it.
func (g *URLTestGroup) balanced(ctx context.Context, network string) (adapter.Outbound, func()) {
	b := g.balance
	if b == nil {
		return nil, nil
	}
	key := balanceKey(ctx)
	now := time.Now()
	b.access.Lock()
	defer b.access.Unlock()
	members := b.tcp
	if network == N.NetworkUDP {
		members = b.udp
	}
	if len(members) == 0 {
		return nil, nil
	}
	if now.Sub(b.lastSweep) > balanceForget/6 {
		b.lastSweep = now
		for oldKey, device := range b.devices {
			if device.active == 0 && now.Sub(device.last) > balanceForget {
				delete(b.devices, oldKey)
			}
		}
	}
	device := b.devices[key]
	if device == nil {
		device = &balanceDevice{}
		b.devices[key] = device
	}
	keep := device.member != nil && slices.Contains(g.outbounds, device.member) && g.memberAlive(device.member) &&
		(slices.Contains(members, device.member) || device.active > 0 || now.Sub(device.last) < g.loss.balanceIdle)
	member := device.member
	switch {
	case keep && slices.Contains(member.Network(), network):
	case keep:
		// its member carries no UDP: this connection goes by another, the device keeps its own
		member = rendezvous(key, members)
	default:
		member = rendezvous(key, members)
		device.member = member
	}
	device.active++
	device.last = now
	var once sync.Once
	return member, func() {
		once.Do(func() {
			b.access.Lock()
			device.active--
			device.last = time.Now()
			b.access.Unlock()
		})
	}
}

// Mode is the mode of the group: fastest, loss or balance.
func (s *URLTest) Mode() string {
	return s.mode
}

// BalanceMember is a member of the balance and the devices it carries.
type BalanceMember struct {
	Tag     string `json:"name"`
	Devices int    `json:"devices"`
}

// Balance returns the members of the balance for TCP and the devices with open connections
// on each; nil when the group does not balance.
func (s *URLTest) Balance() []BalanceMember {
	g := s.group
	if g == nil || g.balance == nil {
		return nil
	}
	b := g.balance
	b.access.Lock()
	defer b.access.Unlock()
	members := make([]BalanceMember, len(b.tcp))
	index := make(map[adapter.Outbound]int, len(b.tcp))
	for i, member := range b.tcp {
		members[i].Tag = member.Tag()
		index[member] = i
	}
	for _, device := range b.devices {
		if i, inBalance := index[device.member]; inBalance && device.active > 0 {
			members[i].Devices++
		}
	}
	return members
}
