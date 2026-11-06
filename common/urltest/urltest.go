package urltest

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
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
	s.notifyUpdated()
	s.access.Unlock()
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
	s.failures[tag] = at
	s.notifyUpdated()
	s.access.Unlock()
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
