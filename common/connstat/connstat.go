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
