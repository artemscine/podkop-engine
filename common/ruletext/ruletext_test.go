package ruletext

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRead(t *testing.T) {
	t.Parallel()
	ruleSet, skipped, err := Read([]byte("# a list\nexample.com\n.Sub.Example.ORG\n+.x.example\n  .ru  \n10.0.0.0/8\n192.0.2.1 # one address\n2001:db8::/32 // addresses\nnot a domain\nwords\nexample.com\nlast.example"))
	require.NoError(t, err)
	require.Len(t, ruleSet.Options.Rules, 2)
	require.Equal(t, []string{"example.com", "sub.example.org", "x.example", "ru", "last.example"}, []string(ruleSet.Options.Rules[0].DefaultOptions.DomainSuffix))
	require.Equal(t, []string{"10.0.0.0/8", "192.0.2.1/32", "2001:db8::/32"}, []string(ruleSet.Options.Rules[1].DefaultOptions.IPCIDR))
	require.Equal(t, Skipped{Count: 2, First: "not a domain"}, skipped)

	// the last line without a line end is read; a list of names alone has one rule
	ruleSet, _, err = Read([]byte("only.example"))
	require.NoError(t, err)
	require.Len(t, ruleSet.Options.Rules, 1)

	// a page served in place of the list is no list
	_, skipped, err = Read([]byte("<html><body>\nAccess to this resource is blocked\nForbidden\n</body></html>\n"))
	require.Error(t, err)
	require.Equal(t, 4, skipped.Count)
	_, _, err = Read([]byte("\n# nothing\n"))
	require.Error(t, err)
}
