//go:build windows

package net

import (
	stdlibnet "net"
	"net/netip"
	"os"
	"sync"
	"testing"
	"time"
)

// selfSocket opens a loopback pair owned by this test process and returns the client's
// local port, so the kernel socket table has a row we can expect to be attributed here.
func selfSocket(t *testing.T) uint16 {
	t.Helper()

	ln, err := stdlibnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot listen on loopback: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	conn, err := stdlibnet.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial to %s failed: %v", ln.Addr(), err)
	}
	t.Cleanup(func() { conn.Close() })

	accepted := make(chan struct{})
	go func() {
		srv, err := ln.Accept()
		if err != nil {
			return
		}
		close(accepted)
		time.Sleep(2 * time.Second)
		srv.Close()
	}()
	<-accepted

	tp, ok := conn.LocalAddr().(*stdlibnet.TCPAddr)
	if !ok {
		t.Fatalf("unexpected local address type %T", conn.LocalAddr())
	}
	return uint16(tp.Port)
}

func TestFindProcessCachedResolvesOwnSocket(t *testing.T) {
	clientPort := selfSocket(t)
	want := os.Getpid()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		pid, name, path, err := FindProcessCached("tcp", "127.0.0.1", clientPort, "127.0.0.1", 80)
		if err == nil {
			if pid != want {
				t.Fatalf("attribution went to pid %d, expected %d", pid, want)
			}
			if name == "" || path == "" {
				t.Fatalf("resolved pid %d but no image: name=%q path=%q", pid, name, path)
			}
			t.Logf("own socket attributed to pid %d (%s)", pid, name)
			return
		}
		time.Sleep(100 * time.Millisecond) // the shared snapshot lags a new socket by one TTL
	}
	t.Fatalf("own socket on port %d was never attributed", clientPort)
}

func TestFindProcessCachedIgnoresForeignSource(t *testing.T) {
	if _, _, _, err := FindProcessCached("tcp", "203.0.113.7", 41234, "127.0.0.1", 80); err != ErrNotLocal {
		t.Fatalf("a remote source must not be resolved locally, got %v", err)
	}
}

// TestFindProcessCachedSwapUnderLoad forces snapshot swaps while readers probe the table,
// which is where the double buffered view could break; run it under -race.
func TestFindProcessCachedSwapUnderLoad(t *testing.T) {
	clientPort := selfSocket(t)
	key := processSocketKey{network: processNetTCP, address: netip.MustParseAddr("127.0.0.1"), port: clientPort}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var hits int64
	var hitsMu sync.Mutex

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			processLookup.mu.Lock()
			processLookup.tableAt = time.Time{} // pretend the snapshot just aged out
			processLookup.mu.Unlock()
			time.Sleep(time.Millisecond)
		}
	}()

	for range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if entry, found := processLookup.lookup(key); found {
					hitsMu.Lock()
					hits++
					hitsMu.Unlock()
					if entry.pid != uint32(os.Getpid()) {
						t.Errorf("swapped table returned pid %d for our own socket", entry.pid)
						return
					}
				}
			}
		}()
	}

	time.Sleep(600 * time.Millisecond)
	close(stop)
	wg.Wait()

	processLookup.mu.RLock()
	rows := len(processLookup.live)
	processLookup.mu.RUnlock()

	if rows == 0 {
		t.Fatal("live table emptied itself under concurrent swaps")
	}
	if hits == 0 {
		t.Fatal("our own socket was never found across many swaps")
	}
	t.Logf("%d hits against a %d row table under continuous swaps", hits, rows)
}
