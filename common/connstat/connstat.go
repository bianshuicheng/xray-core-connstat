// Package connstat provides per-connection traffic tracking.
//
// The dispatcher registers a pair of counters for every dispatched connection
// and attaches them to the outbound context; the internet dialer wraps the
// dialed connection in a stat.CounterConnection so that every byte read or
// written on the wire is counted - including raw-copy fast paths, because
// UnwrapRawConn penetrates stat.CounterConnection and hands the counters back
// to the copy loop (see proxy.CopyRawConnIfExist).
//
// Counter names are visible through the existing StatsService gRPC API:
//
//	conn>>><id>|<dest>|<inboundTag>|<outboundTag>>>uplink
//	conn>>><id>|<dest>|<inboundTag>|<outboundTag>>>downlink
package connstat

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/xtls/xray-core/features/stats"
)

type ctxKey struct{}

// Counters carries the per-connection counters through the outbound context.
type Counters struct {
	ID   int64
	Up   stats.Counter // bytes sent to the server (client -> server)
	Down stats.Counter // bytes received from the server (server -> client)
}

// ContextWithCounters returns a copy of ctx carrying cs.
func ContextWithCounters(ctx context.Context, cs *Counters) context.Context {
	return context.WithValue(ctx, ctxKey{}, cs)
}

// FromContext returns the per-connection counters attached to ctx, if any.
func FromContext(ctx context.Context) *Counters {
	if cs, ok := ctx.Value(ctxKey{}).(*Counters); ok {
		return cs
	}
	return nil
}

var connID int64

// NextID returns a process-wide unique connection id.
func NextID() int64 {
	return atomic.AddInt64(&connID, 1)
}

// ProcessInfo describes the local process that opened a connection.
type ProcessInfo struct {
	PID  int
	Name string
	Path string
}

var (
	processMu    sync.RWMutex
	processTable = map[int64]ProcessInfo{}
)

// SetProcess records the owning process of a connection.
func SetProcess(id int64, info ProcessInfo) {
	processMu.Lock()
	defer processMu.Unlock()
	processTable[id] = info
}

// ProcessOf returns the owning process of a connection, if known.
func ProcessOf(id int64) (ProcessInfo, bool) {
	processMu.RLock()
	defer processMu.RUnlock()
	info, ok := processTable[id]
	return info, ok
}

// RemoveProcess drops the process record of a closed connection.
func RemoveProcess(id int64) {
	processMu.Lock()
	defer processMu.Unlock()
	delete(processTable, id)
}
