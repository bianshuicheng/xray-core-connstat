//go:build windows

package tun

import (
	"net/netip"
	"sync"
	"time"

	"golang.org/x/sys/windows"
	"golang.zx2c4.com/wireguard/windows/tunnel/winipcfg"
)

const egressCacheTTL = 500 * time.Millisecond

type egressCache struct {
	mu     sync.Mutex
	at     time.Time
	routes []egressRoute
}

var egress egressCache

func (c *egressCache) current() ([]egressRoute, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.routes) > 0 && time.Since(c.at) <= egressCacheTTL {
		return c.routes, nil
	}

	rows, err := winipcfg.GetIPForwardTable2(windows.AF_UNSPEC)
	if err != nil {
		return nil, err
	}
	routes := make([]egressRoute, 0, len(rows))
	for i := range rows {
		row := &rows[i]
		addr := row.DestinationPrefix.RawPrefix.Addr()
		if !addr.IsValid() {
			continue
		}
		routes = append(routes, egressRoute{
			prefix: netip.PrefixFrom(addr, int(row.DestinationPrefix.PrefixLength)),
			metric: row.Metric,
			index:  int(row.InterfaceIndex),
		})
	}
	c.routes, c.at = routes, time.Now()
	return c.routes, nil
}

// egressIsTun asks, for this particular destination, which interface the system would use
// right now. The interface chosen when the TUN came up is not an answer: when that adapter
// dies Windows ignores the bind and takes whatever default route is left, and during an
// outage that is our own TUN - the core then dials into the stack that is dialling.
func egressIsTun(dest netip.Addr, tunIndex int) bool {
	if tunIndex <= 0 || !dest.IsValid() {
		return false
	}
	routes, err := egress.current()
	if err != nil {
		return false
	}
	best, found := bestEgress(dest, routes)
	return found && best.index == tunIndex
}
