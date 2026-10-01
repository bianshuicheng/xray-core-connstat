//go:build windows

package net

import (
	"encoding/binary"
	"net/netip"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"golang.org/x/sys/windows"
)

const (
	// Enumerating the kernel socket table once per lookup is far too expensive the
	// moment thousands of connections arrive together (TUN reconnect storms), so the
	// table is reused for a short window and the pid to image lookup is memoised.
	processTableTTL  = 500 * time.Millisecond
	processImageTTL  = 30 * time.Second
	processImageMax  = 4096
	processSocketMax = 128 * 1024
)

const (
	processNetTCP uint8 = iota
	processNetUDP
)

type processSocketKey struct {
	network uint8
	address netip.Addr
	port    uint16
}

type processSocketEntry struct {
	pid  uint32
	rank int
}

type processImageEntry struct {
	name string
	path string
	at   time.Time
}

// processTableSpec describes one GetExtended*Table result layout.
type processTableSpec struct {
	fn      uintptr
	family  int
	class   int
	network uint8
	item    int
	ip      int
	ipSize  int
	port    int
	pid     int
	state   int // negative when the table has no state column (UDP)
}

// processLookupCache keeps two socket tables and swaps them: the live one is only ever
// read, the idle one is rebuilt outside any lock, and only the swap needs the writers
// exclusive. Readers therefore never queue behind a table walk, which is what made a
// reconnect storm serialize on the lock.
type processLookupCache struct {
	mu      sync.RWMutex
	live    map[processSocketKey]processSocketEntry
	spare   map[processSocketKey]processSocketEntry
	tableAt time.Time
	refresh atomic.Bool

	imagesMu sync.RWMutex
	images   map[uint32]processImageEntry
}

var processLookup = &processLookupCache{
	live:   make(map[processSocketKey]processSocketEntry, 1024),
	spare:  make(map[processSocketKey]processSocketEntry, 1024),
	images: make(map[uint32]processImageEntry, 64),
}

// FindProcessCached answers the same question as FindProcess from a shared, briefly
// cached view of the socket table. Attribution can therefore lag a brand new socket by
// up to processTableTTL, which is invisible to routing and to a per second panel, and
// turns a connection storm from O(connections x table) into one table read.
func FindProcessCached(network, srcIP string, srcPort uint16, destIP string, destPort uint16) (PID int, Name string, AbsolutePath string, err error) {
	var netID uint8
	switch network {
	case "tcp":
		netID = processNetTCP
	case "udp":
		netID = processNetUDP
	default:
		panic("Unsupported network type for process lookup.")
	}
	// the table procedures are resolved lazily by FindProcess; without this the cached
	// path would call through a nil pointer
	once.Do(func() {
		initErr = initWin32API()
	})
	if initErr != nil {
		return 0, "", "", initErr
	}
	local, err := IsLocal(ParseIP(srcIP))
	if err != nil {
		return 0, "", "", err
	}
	if !local {
		return 0, "", "", ErrNotLocal
	}
	if srcPort == 0 {
		return 0, "", "", errors.New("no source port for process lookup")
	}
	address, err := normalizeProcessAddress(srcIP)
	if err != nil {
		return 0, "", "", err
	}

	key := processSocketKey{network: netID, address: address, port: srcPort}
	entry, found := processLookup.lookup(key)
	if !found && netID == processNetUDP {
		// Windows lists a not explicitly bound UDP socket under the any address while
		// the observed source is the interface address it actually sent from.
		unspecified := netip.AddrFrom4([4]byte{})
		if address.Is6() {
			unspecified = netip.AddrFrom16([16]byte{})
		}
		entry, found = processLookup.lookup(processSocketKey{network: netID, address: unspecified, port: srcPort})
	}
	if !found {
		return 0, "", "", errors.New("not found")
	}

	name, path := processLookup.image(entry.pid)
	if name == "" {
		return int(entry.pid), "", path, errors.New("unable to read process image")
	}
	return int(entry.pid), name, path, nil
}

func (c *processLookupCache) lookup(key processSocketKey) (processSocketEntry, bool) {
	c.mu.RLock()
	entry, found := c.live[key]
	stale := c.tableAt.IsZero() || time.Since(c.tableAt) >= processTableTTL
	c.mu.RUnlock()

	if !stale || !c.refresh.CompareAndSwap(false, true) {
		return entry, found
	}
	// This caller drew the short straw: rebuild the view, then answer from the fresh one
	// so a socket that just appeared is not blamed on its predecessor.
	defer c.refresh.Store(false)
	c.refreshTable()

	c.mu.RLock()
	entry, found = c.live[key]
	c.mu.RUnlock()
	return entry, found
}

func (c *processLookupCache) refreshTable() {
	tables := []processTableSpec{
		{fn: getExTCPTable, family: windows.AF_INET, class: tcpTablePidConn, network: processNetTCP, item: 24, ip: 4, ipSize: 4, port: 8, pid: 20, state: 0},
		{fn: getExTCPTable, family: windows.AF_INET6, class: tcpTablePidConn, network: processNetTCP, item: 56, ip: 0, ipSize: 16, port: 20, pid: 52, state: 48},
		{fn: getExUDPTable, family: windows.AF_INET, class: udpTablePid, network: processNetUDP, item: 12, ip: 0, ipSize: 4, port: 4, pid: 8, state: -1},
		{fn: getExUDPTable, family: windows.AF_INET6, class: udpTablePid, network: processNetUDP, item: 28, ip: 0, ipSize: 16, port: 20, pid: 24, state: -1},
	}

	// Everything expensive happens before the spare map is published, so no reader waits
	// on the syscalls or on the row loop.
	c.mu.Lock()
	next := c.spare
	c.mu.Unlock()
	clear(next)

	count := 0
	for _, table := range tables {
		buf, err := getTransportTable(table.fn, table.family, table.class)
		if err != nil {
			continue
		}
		count += indexProcessTable(next, buf, table)
	}
	if count == 0 || count > processSocketMax {
		return // keep the previous view rather than trusting a nonsense table
	}

	c.mu.Lock()
	c.live, c.spare = c.spare, c.live
	c.tableAt = time.Now()
	c.mu.Unlock()
}

func indexProcessTable(into map[processSocketKey]processSocketEntry, buf []byte, table processTableSpec) int {
	if len(buf) < 4 {
		return 0
	}
	count := int(binary.NativeEndian.Uint32(buf[:4]))
	body := buf[4:]
	for i := 0; i < count; i++ {
		start := i * table.item
		if start+table.item > len(body) {
			return len(into)
		}
		row := body[start : start+table.item]

		rank := 1
		if table.state >= 0 {
			rank = processStateRank(binary.NativeEndian.Uint32(row[table.state : table.state+4]))
		}
		addr, ok := netip.AddrFromSlice(row[table.ip : table.ip+table.ipSize])
		if !ok {
			continue
		}
		port := syscall.Ntohs(uint16(binary.NativeEndian.Uint32(row[table.port : table.port+4])))
		if port == 0 {
			continue
		}
		pid := binary.NativeEndian.Uint32(row[table.pid : table.pid+4])
		if pid == 0 {
			// TIME_WAIT rows carry no owner and would blame the idle process
			continue
		}
		key := processSocketKey{network: table.network, address: addr.Unmap(), port: port}
		if old, found := into[key]; found && old.rank >= rank {
			continue
		}
		into[key] = processSocketEntry{pid: pid, rank: rank}
	}
	return len(into)
}

// processStateRank prefers a socket that is actually carrying data when the same local
// endpoint shows up more than once.
func processStateRank(state uint32) int {
	switch state {
	case 5: // MIB_TCP_STATE_ESTAB
		return 3
	case 2: // SYN_SENT, what a session looks like while the uplink is down
		return 2
	case 1, 3, 4, 6, 7, 8:
		return 1
	default:
		return 0
	}
}

func (c *processLookupCache) image(pid uint32) (string, string) {
	switch pid {
	case 0:
		return "System Idle Process", ""
	case 4:
		return "System", ""
	}

	c.imagesMu.RLock()
	entry, found := c.images[pid]
	c.imagesMu.RUnlock()
	if found && time.Since(entry.at) < processImageTTL {
		return entry.name, entry.path
	}

	name, path := "", ""
	if resolved, err := getExecPathFromPID(pid); err == nil {
		path = filepath.ToSlash(resolved)
		base := filepath.Base(path)
		if extension := filepath.Ext(base); strings.EqualFold(extension, ".exe") {
			base = strings.TrimSuffix(base, extension)
		}
		name = base
	}

	c.imagesMu.Lock()
	if len(c.images) > processImageMax {
		clear(c.images)
	}
	c.images[pid] = processImageEntry{name: name, path: path, at: time.Now()}
	c.imagesMu.Unlock()
	return name, path
}

func normalizeProcessAddress(raw string) (netip.Addr, error) {
	addr, err := netip.ParseAddr(raw)
	if err != nil {
		return netip.Addr{}, errors.New("invalid address for process lookup").Base(err)
	}
	return addr.Unmap(), nil
}
