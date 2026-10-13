package rule

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"

	"github.com/stretchr/testify/require"
)

// An update asked for now takes every remote rule-set, due or not, and tells how many
// got a new list and how many failed; a scheduled update is not made earlier by it.
func TestRuleSetUpdaterUpdateNow(t *testing.T) {
	var served atomic.Int32
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		io.WriteString(w, testSourceSet)
	}))
	defer good.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "gone", http.StatusNotFound)
	}))
	defer bad.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sets := make([]adapter.RuleSet, 2)
	for index, url := range []string{good.URL, bad.URL} {
		set := newTestRuleSet(url)
		set.ctx = ctx
		set.options.Format = C.RuleSetFormatSource
		// downloaded a moment ago: not due for an hour
		set.updateInterval = time.Hour
		set.lastUpdated = time.Now()
		sets[index] = set
	}
	updater := NewRuleSetUpdater(ctx, sets)
	updater.Start()

	time.Sleep(100 * time.Millisecond)
	require.Zero(t, served.Load(), "a rule-set that is not due was updated by the schedule")
	for round := int32(1); round <= 2; round++ {
		summary, err := updater.UpdateNow(ctx)
		require.NoError(t, err)
		require.Equal(t, RuleSetUpdateSummary{Total: 2, Loaded: 1, Failed: 1}, summary)
		require.Equal(t, round, served.Load())
	}
	time.Sleep(100 * time.Millisecond)
	require.EqualValues(t, 2, served.Load(), "the update asked for was followed by one more")

	// one that waits is let go when the caller gives up, and when the updater is closed
	short, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stop()
	require.NoError(t, updater.Close())
	_, err := updater.UpdateNow(short)
	require.Error(t, err)
}

// With no remote rule-set there is no updater, and nothing to wait for.
func TestRuleSetUpdaterWithoutRemoteRuleSets(t *testing.T) {
	require.Nil(t, NewRuleSetUpdater(context.Background(), nil))
}
