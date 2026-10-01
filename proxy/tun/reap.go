package tun

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

// Nothing else in this stack bounds or closes TUN connections: the policy idle timeout is
// read but never applied, gVisor keepalive is switched off, and the forwarder accepts up to
// 65535 concurrent connections. Reconnecting applications do not stop asking, so RSS only
// ever grows. These two rules put a ceiling on how many are alive and a deadline on the ones
// that were never going to carry anything.
const (
	// A connection that asked and never got an answer is what a failed dial looks like from
	// here. mihomo gives every inbound flow a 5 second budget for exactly this and ships it
	// to its whole user base, so 5 s is the borrowed number rather than a guess.
	zombieAfter  = 5 * time.Second
	silentAfter  = 30 * time.Second
	reapInterval = 5 * time.Second

	// A connection that carried traffic and then went quiet is closed after its idle
	// budget. Without this, every session that ever got answered - a DNS reply, a short
	// page fetch, a RST-to-the-app - lives forever, and the table accumulates hundreds of
	// gvisor endpoints and pump goroutines across an outage and normal browsing alike.
	// mihomo's UDP NAT entries expire after about half a minute; TCP gets the policy-grade
	// five minutes xray's own idle timeout would have applied if it were wired up here.
	udpIdleAfter = 60 * time.Second
	tcpIdleAfter = 300 * time.Second

	// 1024 is well above what this machine uses when its uplink is healthy (tens to a few
	// hundred), and it is the thing that makes memory finite during a storm. Clash made the
	// same trade openly: "memory matters more than peak performance".
	maxLiveTun = 1024

	// The reaper is also the outage detector: a tick that reaps this many connections means
	// clients are asking into a dead uplink faster than they can be served. For
	// breakerHold, admission drops to breakerAdmitPerSec per second - reconnect storms stop
	// parking hundreds of sessions, while the trickle keeps probing for recovery. The
	// breaker expires on its own; a healthy uplink simply never re-trips it.
	breakerTripThreshold = 15
	breakerHold          = 30 * time.Second
	breakerAdmitPerSec   = 2
)

// tripBreaker closes the admission gate for breakerHold. Called the moment the dialer
// controller finds there is no working egress - one refused dial is enough to know the
// storm is starting, long before the reaper can count fifteen corpses.
func tripBreaker() {
	breakerUntil.Store(time.Now().Add(breakerHold).Unix())
}

type watchedConn struct {
	net.Conn
	started   time.Time
	udp       bool
	everRead  atomic.Bool
	everWrote atomic.Bool
	lastActive atomic.Int64 // unix nanos of the last read or write that moved data
}

var (
	reapMu     sync.Mutex
	reapedConn = map[*watchedConn]struct{}{}
	reaped     atomic.Int64
	overCap    atomic.Int64
	reapWarn   atomic.Int64
	reapOnce   sync.Once

	breakerUntil   atomic.Int64 // unix seconds; admission is rate-limited until then
	breakerRefused atomic.Int64
	admitSecond    atomic.Int64
	admitCount     atomic.Int64
)

func (w *watchedConn) Read(b []byte) (int, error) {
	n, err := w.Conn.Read(b)
	if n > 0 {
		w.everRead.Store(true)
		w.lastActive.Store(time.Now().UnixNano())
	}
	return n, err
}

func (w *watchedConn) Write(b []byte) (int, error) {
	n, err := w.Conn.Write(b)
	if n > 0 {
		w.everWrote.Store(true)
		w.lastActive.Store(time.Now().UnixNano())
	}
	return n, err
}

// isZombie is the whole reaping rule. Asking and never getting an answer is the failure we
// want gone quickly; a socket the application never wrote on gets the long grace, because
// plenty of real clients open early and speak late. A session that was answered is a real
// conversation - it ends when it goes quiet for its idle budget, not before.
func isZombie(w *watchedConn, now time.Time) bool {
	age := now.Sub(w.started)
	if !w.everRead.Load() {
		return age >= silentAfter
	}
	if w.everWrote.Load() {
		idle := now.Sub(time.Unix(0, w.lastActive.Load()))
		if w.udp {
			return idle >= udpIdleAfter
		}
		return idle >= tcpIdleAfter
	}
	return age >= zombieAfter
}

// watchDownstream registers a TUN connection for the sweeper, or refuses it once the
// ceiling is reached - refusing is what keeps the buffer pool from growing to the storm.
func watchDownstream(conn net.Conn, udp bool) (net.Conn, func(), bool) {
	reapOnce.Do(startReaper)

	// Circuit breaker admission gate: while tripped, only a couple of connections per
	// second get in, everything else is closed at the door. The per-second budget is
	// advisory (two goroutines inside the same second may both slip through) - close
	// enough, and no lock on the hot path.
	if now := time.Now(); now.Unix() < breakerUntil.Load() {
		if sec := now.Unix(); admitSecond.Load() != sec {
			admitSecond.Store(sec)
			admitCount.Store(0)
		}
		if admitCount.Add(1) > breakerAdmitPerSec {
			breakerRefused.Add(1)
			return nil, nil, false
		}
	}

	w := &watchedConn{
		Conn:    conn,
		started: time.Now(),
		udp:     udp,
	}
	w.lastActive.Store(time.Now().UnixNano())

	reapMu.Lock()
	if len(reapedConn) >= maxLiveTun {
		reapMu.Unlock()
		overCap.Add(1)
		return nil, nil, false
	}
	reapedConn[w] = struct{}{}
	reapMu.Unlock()

	return w, func() {
		reapMu.Lock()
		delete(reapedConn, w)
		reapMu.Unlock()
	}, true
}

// liveConnections is what the ceiling counts against. Exposed next to over_cap so a
// discrepancy between "refused" and the panel's flow count can be told apart: leaked table
// entries would show here as a number stuck at the ceiling.
func liveConnections() int {
	reapMu.Lock()
	defer reapMu.Unlock()
	return len(reapedConn)
}

func startReaper() {
	go func() {
		ticker := time.NewTicker(reapInterval)
		defer ticker.Stop()

		for range ticker.C {
			now := time.Now()
			var dead []*watchedConn

			reapMu.Lock()
			for w := range reapedConn {
				if isZombie(w, now) {
					delete(reapedConn, w)
					dead = append(dead, w)
				}
			}
			reapMu.Unlock()

			if len(dead) == 0 {
				continue
			}
			total := reaped.Add(int64(len(dead)))
			// Trip the breaker when a single tick reaps storm volumes; the 5 s tick is the
			// rate window, so no extra bookkeeping is needed.
			if len(dead) >= breakerTripThreshold {
				breakerUntil.Store(now.Add(breakerHold).Unix())
			}
			if now.Unix()-reapWarn.Load() > 30 {
				reapWarn.Store(now.Unix())
				errors.LogWarning(context.Background(), "[tun] closed ", len(dead), " dead connection(s), ", total, " in total, ", overCap.Load(), " refused at the ceiling, ", breakerRefused.Load(), " refused by the breaker")
			}
			for _, w := range dead {
				_ = w.Conn.Close()
			}
		}
	}()
}
