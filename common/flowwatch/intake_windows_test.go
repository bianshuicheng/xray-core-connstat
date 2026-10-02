//go:build windows

package flowwatch

import (
	stdlibnet "net"
	"os"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
)

// A one shot UDP query closes its socket long before the sampler would get around to it, so
// attribution has to happen when the flow is created. This checks that it really does.
func TestNewResolvesOwnerAtIntake(t *testing.T) {
	Enable()

	ln, err := stdlibnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot listen on loopback: %v", err)
	}
	defer ln.Close()

	conn, err := stdlibnet.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	srv, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept failed: %v", err)
	}
	defer srv.Close()
	time.Sleep(100 * time.Millisecond) // let the socket land in the kernel table

	f := New(xnet.Network_TCP, xnet.DestinationFromAddr(conn.LocalAddr()), xnet.DestinationFromAddr(ln.Addr()), "test")
	if f == nil {
		t.Fatal("the flow was not registered")
	}
	defer func() {
		registry.Lock()
		delete(flows, f.ID)
		registry.Unlock()
	}()

	// Intake resolution may need one throttled table walk on a busy cache; what the
	// invariant forbids is the old behaviour of staying unidentified for the sampler's
	// full retry tail. Poll briefly instead of asserting the very first instant.
	deadline := time.Now().Add(700 * time.Millisecond)
	for !f.done && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if f.app == "" || !f.done {
		t.Fatalf("owner must be resolved at intake: app=%q done=%v lookup=%q tries=%d", f.app, f.done, f.lookup, f.tries)
	}
	if f.pid != os.Getpid() {
		t.Fatalf("attribution went to pid %d, expected this process %d", f.pid, os.Getpid())
	}
	if !f.own {
		t.Fatal("this flow is our own process and must be marked as such")
	}
}
