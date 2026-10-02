package tun

import (
	"io"
	"net"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
)

type fakeConn struct {
	net.Conn
	written int
	read    int
	closed  bool
}

func (f *fakeConn) Write(b []byte) (int, error) {
	f.written += len(b)
	return len(b), nil
}

func (f *fakeConn) Read(b []byte) (int, error) {
	f.read++
	return 0, io.EOF
}

func (f *fakeConn) Close() error {
	f.closed = true
	return nil
}

func TestWatchDownstreamTracksAndUnregisters(t *testing.T) {
	inner := &fakeConn{}
	wrapped, release, admitted := watchDownstream(inner, false, xnet.TCPDestination(xnet.ParseAddress("127.0.0.1"), 80))
	if !admitted {
		t.Fatal("a free table must admit the connection")
	}

	w, ok := wrapped.(*watchedConn)
	if !ok {
		t.Fatalf("watchDownstream returned %T, want the watched wrapper", wrapped)
	}
	if w.everWrote.Load() || w.everRead.Load() {
		t.Fatal("a fresh connection has not carried anything yet")
	}
	if n, err := w.Write([]byte("hello")); n != 5 || err != nil {
		t.Fatalf("write through the wrapper: n=%d err=%v", n, err)
	}
	if !w.everWrote.Load() {
		t.Fatal("serving bytes must clear the zombie flag")
	}

	release()
	reapMu.Lock()
	_, stillTracked := reapedConn[w]
	reapMu.Unlock()
	if stillTracked {
		t.Fatal("releasing must unregister the connection")
	}
}

func TestIsZombieJudgesBothTiers(t *testing.T) {
	now := time.Now()

	// Asked, never answered: the fast tier.
	unanswered := &watchedConn{Conn: &fakeConn{}, started: now.Add(-zombieAfter - time.Second)}
	unanswered.everRead.Store(true)
	if !isZombie(unanswered, now) {
		t.Fatal("a request that never got an answer should be reaped on the short budget")
	}
	justAsked := &watchedConn{Conn: &fakeConn{}, started: now.Add(-zombieAfter / 2)}
	justAsked.everRead.Store(true)
	if isZombie(justAsked, now) {
		t.Fatal("a slow server still has its budget")
	}

	// Never wrote anything: the patient tier.
	silent := &watchedConn{Conn: &fakeConn{}, started: now.Add(-silentAfter - time.Second)}
	if !isZombie(silent, now) {
		t.Fatal("an idle socket the app never used should eventually be dropped")
	}
	recentlyOpened := &watchedConn{Conn: &fakeConn{}, started: now.Add(-2 * zombieAfter)}
	if isZombie(recentlyOpened, now) {
		t.Fatal("clients that open early and speak late must survive the fast tier")
	}

	// Answered at least once: alive until it goes quiet past its idle budget.
	answered := &watchedConn{Conn: &fakeConn{}, started: now.Add(-10 * time.Minute), udp: false}
	answered.everRead.Store(true)
	answered.everWrote.Store(true)
	answered.lastActive.Store(now.Add(-tcpIdleAfter - time.Second).UnixNano())
	if !isZombie(answered, now) {
		t.Fatal("an answered TCP connection quiet past its idle budget should be reaped")
	}
	busy := &watchedConn{Conn: &fakeConn{}, started: now.Add(-10 * time.Minute), udp: false}
	busy.everRead.Store(true)
	busy.everWrote.Store(true)
	busy.lastActive.Store(now.Add(-time.Second).UnixNano())
	if isZombie(busy, now) {
		t.Fatal("an answered connection with recent traffic is alive")
	}
	answeredUDP := &watchedConn{Conn: &fakeConn{}, started: now.Add(-10 * time.Minute), udp: true}
	answeredUDP.everRead.Store(true)
	answeredUDP.everWrote.Store(true)
	answeredUDP.lastActive.Store(now.Add(-udpIdleAfter - time.Second).UnixNano())
	if !isZombie(answeredUDP, now) {
		t.Fatal("an answered UDP session quiet past the NAT-style budget should be reaped")
	}
	answeredUDPBusy := &watchedConn{Conn: &fakeConn{}, started: now.Add(-10 * time.Minute), udp: true}
	answeredUDPBusy.everRead.Store(true)
	answeredUDPBusy.everWrote.Store(true)
	answeredUDPBusy.lastActive.Store(now.Add(-udpIdleAfter / 2).UnixNano())
	if isZombie(answeredUDPBusy, now) {
		t.Fatal("a UDP session quiet under its own budget is alive even past the TCP budget")
	}
}
