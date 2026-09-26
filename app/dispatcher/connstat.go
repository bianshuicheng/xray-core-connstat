package dispatcher

import (
	"context"
	"strconv"

	"github.com/xtls/xray-core/common/connstat"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/stats"
)

// connStat carries the counters of one live connection and knows how to
// remove them from the stats manager again.
type connStat struct {
	upName   string
	downName string
	*connstat.Counters
}

func (c *connStat) unregister(sm stats.Manager) {
	sm.UnregisterCounter(c.upName)
	sm.UnregisterCounter(c.downName)
	connstat.RemoveProcess(c.ID)
}

// lookupConnProcess asynchronously records the local process that opened the
// connection (best effort: matches the OS socket table by the inbound source
// ip:port; short-lived connections or LAN sources may not resolve).
func lookupConnProcess(ctx context.Context, id int64, dest net.Destination) {
	inbound := session.InboundFromContext(ctx)
	if inbound == nil || !inbound.Source.IsValid() || inbound.Source.Address == nil {
		return
	}
	network := "tcp"
	if inbound.Source.Network == net.Network_UDP {
		network = "udp"
	}
	srcIP := inbound.Source.Address.IP().String()
	srcPort := uint16(inbound.Source.Port)
	var dstIP string
	var dstPort uint16
	if dest.Address != nil && dest.Address.Family().IsIP() {
		dstIP = dest.Address.IP().String()
		dstPort = uint16(dest.Port)
	}
	go func() {
		if pid, name, path, err := net.FindProcess(network, srcIP, srcPort, dstIP, dstPort); err == nil {
			connstat.SetProcess(id, connstat.ProcessInfo{PID: pid, Name: name, Path: path})
		}
	}()
}

// registerConnStat creates the counter pair of a new connection, exposed as
// conn>>><id>|<dest>|<inboundTag>|<outboundTag>>>uplink/downlink so that any
// StatsService client (e.g. connstat-view) can list live connections with
// their target domain and traffic.
// It returns nil when the stats manager is a no-op (no "stats" in config),
// in which case per-connection tracking is silently disabled.
func (d *DefaultDispatcher) registerConnStat(dest net.Destination, inboundTag, outboundTag string) *connStat {
	if d.stats == nil {
		return nil
	}
	id := connstat.NextID()
	name := "conn>>>" + strconv.FormatInt(id, 10) + "|" + dest.NetAddr() + "|" + inboundTag + "|" + outboundTag + ">>>"
	up, err1 := d.stats.GetOrRegisterCounter(name + "uplink")
	down, err2 := d.stats.GetOrRegisterCounter(name + "downlink")
	if err1 != nil || err2 != nil || up == nil || down == nil {
		return nil
	}
	return &connStat{
		upName:   name + "uplink",
		downName: name + "downlink",
		Counters: &connstat.Counters{ID: id, Up: up, Down: down},
	}
}
