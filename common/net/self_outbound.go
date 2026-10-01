package net

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

// The core must never send its own outbound through the TUN it is serving: that packet is
// captured by the very stack that wrote it, which then treats it as a new client connection
// and dials again. These tables let both ends of that loop recognise themselves without
// going through the OS socket table, which lags by up to one snapshot window.
const (
	selfOutboundTTL = 5 * time.Minute
	selfOutboundMax = 65536
)

var ErrOutboundThroughTun = errors.New("outbound socket is on the TUN interface this core serves")

type selfOutboundKey struct {
	network string
	address string
	port    uint16
}

var selfRefused atomic.Int64

// SelfRefusedCount reports how many outbound dials were refused for leaving through our
// own TUN. Without it the guard's effect is only visible as an absence in netstat.
func SelfRefusedCount() int64 {
	return selfRefused.Load()
}

var selfOutboundTable = struct {
	mu        sync.Mutex
	outbounds map[selfOutboundKey]time.Time
	tunAddrs  map[string]bool
}{
	outbounds: make(map[selfOutboundKey]time.Time, 256),
	tunAddrs:  make(map[string]bool, 4),
}

// RegisterTunAddress records an address that belongs to this core's own TUN interface.
func RegisterTunAddress(address Address) {
	if !address.Family().IsIP() {
		return
	}
	t := &selfOutboundTable
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tunAddrs[address.String()] = true
}

// IsTunAddress reports whether the address belongs to this core's own TUN interface.
// Connections to these are served locally (DNS hijack, gateway) and never need a dial.
func IsTunAddress(address Address) bool {
	if !address.Family().IsIP() {
		return false
	}
	t := &selfOutboundTable
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.tunAddrs[address.String()]
}

// NoteSelfOutbound refuses a socket that left through our own TUN and otherwise remembers
// it, so an incoming TUN connection that is really this dial coming back can be dropped.
func NoteSelfOutbound(network string, address Address, port uint16) error {
	t := &selfOutboundTable
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.tunAddrs[address.String()] {
		selfRefused.Add(1)
		return ErrOutboundThroughTun
	}
	if port == 0 {
		return nil
	}
	now := time.Now()
	if len(t.outbounds) >= selfOutboundMax {
		for k, at := range t.outbounds {
			if now.Sub(at) > selfOutboundTTL {
				delete(t.outbounds, k)
			}
		}
		if len(t.outbounds) >= selfOutboundMax {
			clear(t.outbounds)
		}
	}
	t.outbounds[selfOutboundKey{network: network, address: address.String(), port: port}] = now
	return nil
}

// IsSelfOutbound reports whether a fresh TUN connection is one of this core's own dials
// that has looped back into the TUN.
func IsSelfOutbound(network string, address Address, port uint16) bool {
	if port == 0 || !address.Family().IsIP() {
		return false
	}
	t := &selfOutboundTable
	t.mu.Lock()
	defer t.mu.Unlock()

	key := selfOutboundKey{network: network, address: address.String(), port: port}
	at, found := t.outbounds[key]
	if !found {
		return false
	}
	if time.Since(at) > selfOutboundTTL {
		delete(t.outbounds, key)
		return false
	}
	return true
}
