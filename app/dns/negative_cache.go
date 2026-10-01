package dns

import (
	"strings"
	"sync"
	"time"

	"github.com/xtls/xray-core/features/dns"
)

const (
	negativeCacheTTL = 5 * time.Second
	negativeCacheCap = 4096
)

// negativeCache remembers failed lookups. Without it, every retry from every
// app during an outage walks the whole nameserver chain again.
type negativeCache struct {
	mu     sync.Mutex
	expiry map[string]time.Time
	ring   []string
	next   int
}

func newNegativeCache() *negativeCache {
	return &negativeCache{
		expiry: make(map[string]time.Time, negativeCacheCap),
		ring:   make([]string, negativeCacheCap),
	}
}

func negativeCacheKey(domain string, option dns.IPOption) string {
	switch {
	case option.IPv4Enable && option.IPv6Enable:
		return strings.ToLower(domain) + "|dual"
	case option.IPv6Enable:
		return strings.ToLower(domain) + "|6"
	default:
		return strings.ToLower(domain) + "|4"
	}
}

func (c *negativeCache) hit(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	exp, ok := c.expiry[key]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(c.expiry, key)
		return false
	}
	return true
}

func (c *negativeCache) put(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	exp := time.Now().Add(negativeCacheTTL)
	if _, ok := c.expiry[key]; ok {
		c.expiry[key] = exp
		return
	}
	if old := c.ring[c.next]; old != "" {
		delete(c.expiry, old)
	}
	c.ring[c.next] = key
	c.expiry[key] = exp
	c.next = (c.next + 1) % negativeCacheCap
}
