package sharelink

import (
	"net/url"
	"strings"
	"testing"

	"github.com/sagernet/sing/common/json"
	"github.com/stretchr/testify/require"
)

const testFragmentMask = `{"tcp":[{"type":"fragment","settings":{"packets":"1-3","length":"100-200","delay":"1-3"}}]}`

func TestFinalMaskFragmentInLinks(t *testing.T) {
	t.Parallel()
	for _, transport := range []string{"tcp", "xhttp"} {
		for _, security := range []string{"tls", "reality"} {
			t.Run(transport+"/"+security, func(t *testing.T) {
				content, warnings := decodeJSON(t, policyBase+"security="+security+"&pbk=K&type="+transport+"&fm="+url.QueryEscape(testFragmentMask))
				require.Empty(t, warnings)
				var parsed map[string]json.RawMessage
				require.NoError(t, json.Unmarshal([]byte(content), &parsed))
				var tlsOptions map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(parsed["tls"], &tlsOptions))
				require.JSONEq(t, testFragmentMask, string(tlsOptions["finalmask"]))
			})
		}
	}
}

func TestFinalMaskIntervalIsNotDelay(t *testing.T) {
	t.Parallel()
	mask := strings.Replace(testFragmentMask, `"delay"`, `"interval"`, 1)
	content, warnings := decodeJSON(t, policyBase+"security=tls&fm="+url.QueryEscape(mask))
	require.Contains(t, warnings, "fm.fragment: interval is not supported and is ignored")
	require.Contains(t, content, `"finalmask"`)
	require.NotContains(t, content, `"interval"`)
	require.NotContains(t, content, `"delay"`)
}

func TestFinalMaskArraysAndMaxSplit(t *testing.T) {
	t.Parallel()
	mask := `{"tcp":[{"type":"fragment","settings":{"packets":"tlshello","lengths":["0","5-10","20-30"],"delays":["1","2-4"],"maxSplit":"3-6"}}]}`
	content, warnings := decodeJSON(t, policyBase+"security=tls&fm="+url.QueryEscape(mask))
	require.Empty(t, warnings)
	require.Contains(t, content, `"lengths"`)
	require.Contains(t, content, `"maxSplit":"3-6"`)
}

func TestFinalMaskDownloadStreamIsIndependent(t *testing.T) {
	t.Parallel()
	extra := `{"downloadSettings":{"address":"download.example.org","port":443,"security":"tls","tlsSettings":{"serverName":"download.example.org","fingerprint":"firefox"},"finalmask":` + testFragmentMask + `}}`
	content, warnings := decodeJSON(t, policyBase+"security=tls&type=xhttp&extra="+url.QueryEscape(extra))
	require.Empty(t, warnings)
	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(content), &out))
	mainTLS := out["tls"].(map[string]any)
	require.NotContains(t, mainTLS, "finalmask")
	down := out["transport"].(map[string]any)["download"].(map[string]any)["tls"].(map[string]any)
	require.Contains(t, down, "finalmask")

	extra = `{"downloadSettings":{"address":"download.example.org","port":443,"security":"tls"}}`
	content, warnings = decodeJSON(t, policyBase+"security=tls&type=xhttp&fm="+url.QueryEscape(testFragmentMask)+"&extra="+url.QueryEscape(extra))
	require.Empty(t, warnings)
	require.NoError(t, json.Unmarshal([]byte(content), &out))
	require.Contains(t, out["tls"].(map[string]any), "finalmask")
	down = out["transport"].(map[string]any)["download"].(map[string]any)["tls"].(map[string]any)
	require.NotContains(t, down, "finalmask")
}

func TestFinalMaskUnsupportedOrInvalidRefused(t *testing.T) {
	t.Parallel()
	for _, suffix := range []string{
		"security=none&fm=" + url.QueryEscape(testFragmentMask),
		"security=tls&type=quic&fm=" + url.QueryEscape(testFragmentMask),
		"security=tls&type=xhttp&alpn=h3&fm=" + url.QueryEscape(testFragmentMask),
		"security=tls&type=xhttp&sb.tls.alpn=h3&fm=" + url.QueryEscape(testFragmentMask),
		"security=tls&sb.tls.enabled=0&fm=" + url.QueryEscape(testFragmentMask),
		"security=tls&sb.tls.fragment=1&fm=" + url.QueryEscape(testFragmentMask),
		"security=tls&sb.tls.record_fragment=1&fm=" + url.QueryEscape(testFragmentMask),
		"security=tls&fm=" + url.QueryEscape(`{"tcp":[{"type":"fragment","settings":{"length":"0"}}]}`),
		"security=tls&fm=" + url.QueryEscape(`{"tcp":[{"type":"sudoku"}]}`),
		"security=tls&fm=" + url.QueryEscape(`{"udp":[{"type":"fragment"}]}`),
	} {
		require.Equal(t, "finalmask", refusalID(t, policyBase+suffix))
	}
}

func TestFinalMaskDownloadHTTP3Refused(t *testing.T) {
	t.Parallel()
	extra := `{"downloadSettings":{"address":"download.example.org","port":443,"security":"tls","tlsSettings":{"alpn":["h3"]},"finalmask":` + testFragmentMask + `}}`
	require.Equal(t, "finalmask", refusalID(t, policyBase+"security=tls&type=xhttp&extra="+url.QueryEscape(extra)))
}

func TestFinalMaskErrorDoesNotEchoLinkValues(t *testing.T) {
	t.Parallel()
	const secret = "do-not-echo-this-value"
	for _, field := range []string{"length", "packets", "delay", "maxSplit"} {
		mask := `{"tcp":[{"type":"fragment","settings":{"length":"10-20","` + field + `":"` + secret + `"}}]}`
		_, _, err := Decode(policyBase + "security=tls&fm=" + url.QueryEscape(mask))
		require.Error(t, err)
		require.NotContains(t, err.Error(), secret)
	}
}
