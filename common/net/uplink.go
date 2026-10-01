package net

import "sync/atomic"

// uplinkDown records whether outbound dials are currently known to be impossible. The TUN
// dialer controller sets it from every dial it sees: refusing means no working egress was
// found, binding one means there is. Consumers - most importantly the DNS outbound - use it
// to fail fast instead of starting work that can only end in a timeout, which is what turns
// an outage into a storm of half-open sessions and leaked goroutines.
//
// It starts false (assume an uplink) and only ever reflects the most recent dialer verdict,
// so a stale "down" heals itself with the first successful dial.
var uplinkDown atomic.Bool

// SetUplinkDown records the latest egress verdict. Called by the dialer controller.
func SetUplinkDown(down bool) { uplinkDown.Store(down) }

// UplinkDown reports whether the most recent dial verdict said there is no working egress.
func UplinkDown() bool { return uplinkDown.Load() }
