// Package ruletext reads rule-sets written as plain text (format "text"): the lists of
// domains and addresses many sites publish, one item a line, with no rule-set around
// them. Each item becomes a rule of the set: a domain matches itself and its subdomains
// (domain_suffix), an IP address or CIDR matches by destination (ip_cidr). The set is
// matched as any other: the text is only how it is written.
package ruletext

import (
	"bytes"
	"net/netip"
	"strings"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/json/badoption"
)

// Skipped is what a list had that is neither a domain nor an IP address or CIDR.
type Skipped struct {
	// Count is how many lines were left out.
	Count int
	// First is the first of them, cut to 64 bytes.
	First string
}

// Read returns the rule-set of a list written as text, and what it left out. An item is
// a line; "#" and "//" begin a comment that runs to the end of the line, and spaces
// around the item are dropped. A domain has a dot in it, or begins with one (".ru", a
// top-level domain): a word alone is taken for text, not for a name. It may begin with
// "." or "+." (a suffix either way) and is taken in lower case. A list without a single
// item is an error: a page served in place of the list (a block page, a captive
// portal) is no list, and a line of it is no item.
func Read(content []byte) (option.PlainRuleSetCompat, Skipped, error) {
	var (
		domains  []string
		prefixes []string
		skipped  Skipped
	)
	seenDomains := make(map[string]bool)
	seenPrefixes := make(map[string]bool)
	for len(content) > 0 {
		var line []byte
		end := bytes.IndexByte(content, '\n')
		if end < 0 {
			line, content = content, nil
		} else {
			line, content = content[:end], content[end+1:]
		}
		if comment := bytes.IndexByte(line, '#'); comment >= 0 {
			line = line[:comment]
		}
		if comment := bytes.Index(line, []byte("//")); comment >= 0 {
			line = line[:comment]
		}
		item := string(bytes.TrimSpace(line))
		if item == "" {
			continue
		}
		if prefix, isPrefix := parsePrefix(item); isPrefix {
			text := prefix.String()
			if !seenPrefixes[text] {
				seenPrefixes[text] = true
				prefixes = append(prefixes, text)
			}
			continue
		}
		if domain, isDomain := parseDomain(item); isDomain {
			if !seenDomains[domain] {
				seenDomains[domain] = true
				domains = append(domains, domain)
			}
			continue
		}
		if skipped.Count == 0 {
			skipped.First = item
			if len(skipped.First) > 64 {
				skipped.First = skipped.First[:64]
			}
		}
		skipped.Count++
	}
	if len(domains) == 0 && len(prefixes) == 0 {
		if skipped.Count > 0 {
			return option.PlainRuleSetCompat{}, skipped, E.New("no domain and no IP CIDR in the list, the first line: ", skipped.First)
		}
		return option.PlainRuleSetCompat{}, skipped, E.New("no domain and no IP CIDR in the list")
	}
	var rules []option.HeadlessRule
	if len(domains) > 0 {
		rules = append(rules, option.HeadlessRule{Type: C.RuleTypeDefault, DefaultOptions: option.DefaultHeadlessRule{DomainSuffix: badoption.Listable[string](domains)}})
	}
	if len(prefixes) > 0 {
		rules = append(rules, option.HeadlessRule{Type: C.RuleTypeDefault, DefaultOptions: option.DefaultHeadlessRule{IPCIDR: badoption.Listable[string](prefixes)}})
	}
	return option.PlainRuleSetCompat{Version: C.RuleSetVersionCurrent, Options: option.PlainRuleSet{Rules: rules}}, skipped, nil
}

func parsePrefix(item string) (netip.Prefix, bool) {
	if strings.Contains(item, "/") {
		prefix, err := netip.ParsePrefix(item)
		if err != nil {
			return netip.Prefix{}, false
		}
		return prefix.Masked(), true
	}
	address, err := netip.ParseAddr(item)
	if err != nil {
		return netip.Prefix{}, false
	}
	address = address.Unmap()
	return netip.PrefixFrom(address, address.BitLen()), true
}

// parseDomain takes a name of letters, digits, "-" and "_", labels joined by dots, none
// empty and none longer than 63 bytes; a name of one label only as an explicit suffix.
func parseDomain(item string) (string, bool) {
	suffix := strings.HasPrefix(item, ".") || strings.HasPrefix(item, "+.")
	item = strings.TrimPrefix(strings.TrimPrefix(item, "+"), ".")
	item = strings.TrimSuffix(strings.ToLower(item), ".")
	if item == "" || len(item) > 253 || !suffix && !strings.Contains(item, ".") {
		return "", false
	}
	for _, label := range strings.Split(item, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", false
		}
		for index := 0; index < len(label); index++ {
			character := label[index]
			if !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-' || character == '_') {
				return "", false
			}
		}
	}
	return item, true
}
