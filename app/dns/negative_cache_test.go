package dns

import (
	"fmt"
	"testing"
	"time"

	"github.com/xtls/xray-core/features/dns"
)

func TestNegativeCacheRemembersFailures(t *testing.T) {
	c := newNegativeCache()
	key := negativeCacheKey("Example.COM.", dns.IPOption{IPv4Enable: true, IPv6Enable: true})

	if c.hit(key) {
		t.Fatal("fresh cache must not hit")
	}
	c.put(key)
	if !c.hit(key) {
		t.Fatal("failed lookup must be remembered")
	}
	if c.hit(negativeCacheKey("example.com", dns.IPOption{IPv4Enable: true})) {
		t.Fatal("address family is part of the key")
	}
}

func TestNegativeCacheForgetsAfterTTL(t *testing.T) {
	c := newNegativeCache()
	key := negativeCacheKey("a.example", dns.IPOption{IPv4Enable: true})
	c.put(key)

	c.mu.Lock()
	c.expiry[key] = time.Now().Add(-time.Millisecond)
	c.mu.Unlock()

	if c.hit(key) {
		t.Fatal("entry past its TTL must not hit")
	}
	c.mu.Lock()
	_, exists := c.expiry[key]
	c.mu.Unlock()
	if exists {
		t.Fatal("expired entry must be dropped on read")
	}
}

func TestNegativeCacheStaysBounded(t *testing.T) {
	c := newNegativeCache()
	opt := dns.IPOption{IPv4Enable: true}
	oldest := negativeCacheKey("d0.example", opt)
	newest := negativeCacheKey(fmt.Sprintf("d%d.example", negativeCacheCap+99), opt)

	for i := 0; i < negativeCacheCap+100; i++ {
		c.put(negativeCacheKey(fmt.Sprintf("d%d.example", i), opt))
	}

	c.mu.Lock()
	got := len(c.expiry)
	c.mu.Unlock()
	if got > negativeCacheCap {
		t.Fatalf("cache grew to %d, cap is %d", got, negativeCacheCap)
	}
	if c.hit(oldest) {
		t.Fatal("oldest entry should have been evicted")
	}
	if !c.hit(newest) {
		t.Fatal("newest entry must survive")
	}
}
