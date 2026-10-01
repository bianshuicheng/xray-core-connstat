package tun

import "net/netip"

type egressRoute struct {
	prefix netip.Prefix
	metric uint32
	index  int
}

// bestEgress picks the route the system would use for dest: longest matching prefix, then
// the lowest metric. Getting this wrong in the "too true" direction would make destinations
// unreachable, so it is kept free of platform calls and tested on its own.
func bestEgress(dest netip.Addr, routes []egressRoute) (egressRoute, bool) {
	dest = dest.Unmap()
	if !dest.IsValid() {
		return egressRoute{}, false
	}

	var best egressRoute
	found := false
	for _, route := range routes {
		addr := route.prefix.Addr().Unmap()
		if !addr.IsValid() {
			continue
		}
		prefix := netip.PrefixFrom(addr, route.prefix.Bits())
		if !prefix.IsValid() || prefix.Addr().Is4() != dest.Is4() || !prefix.Contains(dest) {
			continue
		}
		if !found || prefix.Bits() > best.prefix.Bits() || (prefix.Bits() == best.prefix.Bits() && route.metric < best.metric) {
			best, found = route, true
		}
	}
	return best, found
}
