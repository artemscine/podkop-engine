package rule

import (
	"context"
	"os"

	"github.com/sagernet/sing/service"
)

// RemoteStart is how the remote rule-sets of a box start (podkop-engine), told by a program that
// embeds sing-box in the context of the box (service.ContextWithPtr). Without it the environment
// tells: Empty is SING_BOX_RULE_SET_START_EMPTY set to anything.
type RemoteStart struct {
	// Empty: a set with nothing cached and no initial file starts empty at once instead of
	// downloading its list during the start. The start downloads the sets five at a time, each up
	// to the start deadline, and the inbounds wait for all of them: a box that replaces another one
	// (a program that applies a new configuration, a restart) is deaf for that long. Starting empty,
	// the box answers at once and the rules that use the set match nothing until the download comes;
	// what they would have matched in the meantime goes by the other rules.
	Empty bool
}

// remoteStartEmpty is RemoteStart.Empty for the box of ctx.
func remoteStartEmpty(ctx context.Context) bool {
	if start := service.PtrFromContext[RemoteStart](ctx); start != nil {
		return start.Empty
	}
	return ruleSetStartEmpty()
}

// ruleSetStartEmpty is what the environment tells. A variable for the tests.
var ruleSetStartEmpty = func() bool {
	return os.Getenv("SING_BOX_RULE_SET_START_EMPTY") != ""
}
