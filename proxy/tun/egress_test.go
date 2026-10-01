package tun

import (
	"net/netip"
	"testing"
)

func pfx(s string) netip.Prefix { return netip.MustParsePrefix(s) }

func TestBestEgressPrefersLongestPrefixThenMetric(t *testing.T) {
	routes := []egressRoute{
		{prefix: pfx("0.0.0.0/0"), metric: 20, index: 17},  // physical default
		{prefix: pfx("0.0.0.0/0"), metric: 100, index: 15}, // TUN default, worse metric
		{prefix: pfx("192.168.2.0/24"), metric: 256, index: 17},
	}

	dest := netip.MustParseAddr("191.222.218.248")
	best, found := bestEgress(dest, routes)
	if !found || best.index != 17 {
		t.Fatalf("healthy default should win by metric, got %+v found=%v", best, found)
	}

	// A LAN destination must not be answered by the default route's interface.
	best, _ = bestEgress(netip.MustParseAddr("192.168.2.20"), routes)
	if best.index != 17 || best.prefix.Bits() != 24 {
		t.Fatalf("the /24 must beat the /0, got %+v", best)
	}
}

func TestBestEgressSeesTheTunWhenNothingElseIsLeft(t *testing.T) {
	routes := []egressRoute{
		{prefix: pfx("0.0.0.0/0"), metric: 100, index: 15}, // only the TUN has a default route
		{prefix: pfx("172.18.0.0/30"), metric: 256, index: 15},
	}
	best, found := bestEgress(netip.MustParseAddr("191.222.218.248"), routes)
	if !found || best.index != 15 {
		t.Fatalf("during an outage the TUN is the egress, got %+v found=%v", best, found)
	}
}

func TestBestEgressIgnoresOtherFamiliesAndGarbage(t *testing.T) {
	routes := []egressRoute{
		{prefix: pfx("::/0"), metric: 0, index: 15},
		{prefix: netip.Prefix{}, metric: 0, index: 99},
	}
	if _, found := bestEgress(netip.MustParseAddr("191.222.218.248"), routes); found {
		t.Fatal("an IPv6-only table must not answer an IPv4 destination")
	}
	if _, found := bestEgress(netip.Addr{}, routes); found {
		t.Fatal("an invalid destination has no egress")
	}
	if _, found := bestEgress(netip.MustParseAddr("10.9.9.9"), routes[:1]); found {
		t.Fatal("no matching route should be reported as a match")
	}
}
