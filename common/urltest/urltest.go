package urltest

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-mux"
	"github.com/sagernet/sing-snell"
	"github.com/sagernet/sing/common"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/ntp"
	"github.com/sagernet/sing/common/observable"
)

type HistoryStorage struct {
	access       sync.RWMutex
	delayHistory map[string]*adapter.URLTestHistory
	// for the block detector (podkop-engine): when the last URL test of a tag failed, while it has
	// passed none since, and what the last download test of a tag found
	failures    map[string]time.Time
	downloads   map[string]DownloadResult
	updateHooks []*observable.Subscriber[struct{}]
	// the delays of a tag the URL tests of the last rounds measured (podkop-engine): its delay
	// is their median, over keeps[tag] rounds
	samples map[string][]Sample
	keeps   map[string]int
	// whether the last URL test of a tag with a URL answered, per URL (podkop-engine): a server
	// may not reach one operator for good (its hoster blocks it), and a URL that answered
	// through it last time is the one to try once more after the URL of a round did not
	links map[string]map[string]bool
}

// Sample is the delay a URL test measured in a round (podkop-engine).
type Sample struct {
	Round int64
	Delay uint16
}

// DownloadResult is what a download test that could run found (podkop-engine).
type DownloadResult struct {
	At       time.Time
	Received int64
	// the body stopped on the way, after Received bytes
	Stopped bool
}

func NewHistoryStorage() *HistoryStorage {
	return &HistoryStorage{
		delayHistory: make(map[string]*adapter.URLTestHistory),
		failures:     make(map[string]time.Time),
		downloads:    make(map[string]DownloadResult),
		samples:      make(map[string][]Sample),
		keeps:        make(map[string]int),
		links:        make(map[string]map[string]bool),
	}
}

func (s *HistoryStorage) AddUpdateHook(hook *observable.Subscriber[struct{}]) {
	s.access.Lock()
	defer s.access.Unlock()
	s.updateHooks = append(s.updateHooks, hook)
}

func (s *HistoryStorage) NotifyUpdated() {
	s.access.RLock()
	defer s.access.RUnlock()
	s.notifyUpdated()
}

func (s *HistoryStorage) LoadURLTestHistory(tag string) *adapter.URLTestHistory {
	if s == nil {
		return nil
	}
	s.access.RLock()
	defer s.access.RUnlock()
	return s.delayHistory[tag]
}

// DeleteURLTestHistory forgets all that was found of a tag: it may be another outbound's by now.
// A failed test is StoreURLTestFailure.
func (s *HistoryStorage) DeleteURLTestHistory(tag string) {
	s.access.Lock()
	delete(s.delayHistory, tag)
	delete(s.failures, tag)
	delete(s.downloads, tag)
	delete(s.samples, tag)
	delete(s.keeps, tag)
	delete(s.links, tag)
	s.notifyUpdated()
	s.access.Unlock()
}

// StoreURLTestLinkResult keeps whether a URL test of a tag with link answered (podkop-engine).
// A failure of the tag as a whole (StoreURLTestFailure) leaves these alone: which URLs answer
// through a server is still known when it comes back.
func (s *HistoryStorage) StoreURLTestLinkResult(tag string, link string, answered bool) {
	s.access.Lock()
	defer s.access.Unlock()
	results := s.links[tag]
	if results == nil {
		results = make(map[string]bool)
		s.links[tag] = results
	}
	results[link] = answered
}

// URLTestLinkAnswered tells whether the last URL test of a tag with link answered, and whether
// one is known at all (podkop-engine).
func (s *HistoryStorage) URLTestLinkAnswered(tag string, link string) (answered bool, known bool) {
	if s == nil {
		return false, false
	}
	s.access.RLock()
	defer s.access.RUnlock()
	answered, known = s.links[tag][link]
	return
}

func (s *HistoryStorage) StoreURLTestHistory(tag string, history *adapter.URLTestHistory) {
	s.access.Lock()
	s.delayHistory[tag] = history
	delete(s.failures, tag)
	s.notifyUpdated()
	s.access.Unlock()
}

// StoreURLTestFailure takes the result of a tag whose URL test failed away, as
// DeleteURLTestHistory does, and keeps the time of the failure (podkop-engine).
func (s *HistoryStorage) StoreURLTestFailure(tag string, at time.Time) {
	s.access.Lock()
	delete(s.delayHistory, tag)
	// what it measured before it went down says nothing of it when it comes back
	delete(s.samples, tag)
	delete(s.keeps, tag)
	s.failures[tag] = at
	s.notifyUpdated()
	s.access.Unlock()
}

// StoreURLTestSample keeps the delay a URL test of a tag measured in a round and returns the
// median of the delays of the last keep rounds, this one among them (podkop-engine). The rounds
// of a group test its members with its URLs in turn, one a round: the median over as many
// rounds as there are URLs is the delay to every operator of them, which the delay to one of
// them is not (a server's way to one operator may be hundreds of milliseconds longer than to
// another), and one stray test does not move it. A round whose URL did not answer through
// the member has no sample: it is left out, not counted as slow. A later sample of the same
// round replaces the earlier one. The median of an even number is the mean of the two
// middle ones.
func (s *HistoryStorage) StoreURLTestSample(tag string, round int64, keep int, delay uint16) uint16 {
	if keep < 1 {
		keep = 1
	}
	s.access.Lock()
	defer s.access.Unlock()
	kept := make([]Sample, 0, keep)
	for _, sample := range s.samples[tag] {
		// a round ahead of this one is of another schedule (a group that was replaced)
		if sample.Round > round-int64(keep) && sample.Round < round {
			kept = append(kept, sample)
		}
	}
	kept = append(kept, Sample{Round: round, Delay: delay})
	s.samples[tag] = kept
	s.keeps[tag] = keep
	return median(kept)
}

// URLTestSmoothed tells whether the delay of a tag is the median of several rounds
// (StoreURLTestSample): one test outside the rounds must not replace it (podkop-engine).
func (s *HistoryStorage) URLTestSmoothed(tag string) bool {
	if s == nil {
		return false
	}
	s.access.RLock()
	defer s.access.RUnlock()
	return s.keeps[tag] > 1 && len(s.samples[tag]) > 0
}

// LoadURLTestSamples returns the delays StoreURLTestSample keeps of a tag (podkop-engine).
func (s *HistoryStorage) LoadURLTestSamples(tag string) []Sample {
	if s == nil {
		return nil
	}
	s.access.RLock()
	defer s.access.RUnlock()
	return slices.Clone(s.samples[tag])
}

func median(samples []Sample) uint16 {
	delays := make([]int, len(samples))
	for i, sample := range samples {
		delays[i] = int(sample.Delay)
	}
	slices.Sort(delays)
	middle := len(delays) / 2
	if len(delays)%2 == 1 {
		return uint16(delays[middle])
	}
	return uint16((delays[middle-1] + delays[middle] + 1) / 2)
}

// LoadURLTestFailure returns when the last URL test of a tag failed, if it has passed none since
// (podkop-engine).
func (s *HistoryStorage) LoadURLTestFailure(tag string) (at time.Time, loaded bool) {
	if s == nil {
		return
	}
	s.access.RLock()
	defer s.access.RUnlock()
	at, loaded = s.failures[tag]
	return
}

// StoreDownloadResult keeps what a download test of a tag found (podkop-engine).
func (s *HistoryStorage) StoreDownloadResult(tag string, result DownloadResult) {
	s.access.Lock()
	s.downloads[tag] = result
	s.access.Unlock()
}

// LoadDownloadResult returns what the last download test of a tag that could run found
// (podkop-engine).
func (s *HistoryStorage) LoadDownloadResult(tag string) (result DownloadResult, loaded bool) {
	if s == nil {
		return
	}
	s.access.RLock()
	defer s.access.RUnlock()
	result, loaded = s.downloads[tag]
	return
}

func (s *HistoryStorage) notifyUpdated() {
	for _, updateHook := range s.updateHooks {
		updateHook.Emit(struct{}{})
	}
}

func (s *HistoryStorage) Close() error {
	s.access.Lock()
	defer s.access.Unlock()
	s.updateHooks = nil
	return nil
}

func URLTest(ctx context.Context, link string, detour N.Dialer) (uint16, error) {
	multiplexOutbound, isMultiplexOutbound := common.Cast[adapter.OutboundWithMultiplex](detour)
	if isMultiplexOutbound && multiplexOutbound.MultiplexEnabled() {
		warmContext := adapter.ContextWithKeepSession(ctx)
		warmContext = mux.ContextWithKeepSession(warmContext)
		warmContext = contextWithQUICKeepSession(warmContext)
		warmContext = snell.ContextWithKeepSession(warmContext)
		_, err := urlTest(warmContext, link, detour)
		if err != nil {
			return 0, err
		}
	}
	return urlTest(ctx, link, detour)
}

func urlTest(ctx context.Context, link string, detour N.Dialer) (t uint16, err error) {
	if link == "" {
		link = "https://www.gstatic.com/generate_204"
	}
	linkURL, err := url.Parse(link)
	if err != nil {
		return
	}
	hostname := linkURL.Hostname()
	port := linkURL.Port()
	if port == "" {
		switch linkURL.Scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		}
	}

	start := time.Now()
	instance, err := detour.DialContext(ctx, "tcp", M.ParseSocksaddrHostPortStr(hostname, port))
	if err != nil {
		return
	}
	defer instance.Close()
	if N.NeedHandshakeForWrite(instance) {
		start = time.Now()
	}
	req, err := http.NewRequest(http.MethodHead, link, nil)
	if err != nil {
		return
	}
	client := probeClient(ctx, instance)
	defer client.CloseIdleConnections()
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return
	}
	resp.Body.Close()
	t = uint16(time.Since(start) / time.Millisecond)
	return
}

// probeClient makes an HTTP client that speaks over instance only, the connection the test
// dialed through the member: no redirects, no compression asked for (a download test counts
// the bytes that cross the link, and a body of zeros would compress to nothing), the TCP
// timeout.
func probeClient(ctx context.Context, instance net.Conn) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return instance, nil
			},
			TLSClientConfig: &tls.Config{
				Time:    ntp.TimeFuncFromContext(ctx),
				RootCAs: adapter.RootPoolFromContext(ctx),
			},
			DisableCompression: true,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Timeout: C.TCPTimeout,
	}
}
