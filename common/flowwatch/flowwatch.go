// Package flowwatch keeps a live table of client-facing flows so that a control
// panel can show, per application, how much traffic is moving through xray right now.
package flowwatch

import (
	"context"
	"math"
	stdnet "net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/transport/internet/stat"
)

const (
	sampleInterval      = time.Second
	rateTau             = 2 * time.Second
	closedRetention     = 2 * time.Second
	maxLiveFlows        = 8192
	maxOwnerPerTick     = 256
	maxOwnerRetries     = 10
	ownerRetryStride    = 5 // after the first ten tries, ask again every fifth pass while open
	maxTargetListPerApp = 12
)

// flowCounter implements stats.Counter on a single atomic int64.
type flowCounter struct {
	value atomic.Int64
}

func (c *flowCounter) Value() int64 { return c.value.Load() }

func (c *flowCounter) Set(v int64) int64 { return c.value.Swap(v) }

func (c *flowCounter) Add(v int64) int64 { return c.value.Add(v) - v }

// joinedCounter feeds one byte count into several counters, used where xray
// already attaches a per tag counter to the same connection.
type joinedCounter struct {
	first  stats.Counter
	second stats.Counter
}

func (j joinedCounter) Value() int64 { return j.first.Value() + j.second.Value() }

func (j joinedCounter) Set(v int64) int64 {
	j.first.Set(v)
	return j.second.Set(v)
}

func (j joinedCounter) Add(v int64) int64 {
	j.first.Add(v)
	return j.second.Add(v)
}

func joinCounter(existing stats.Counter, extra stats.Counter) stats.Counter {
	switch {
	case extra == nil:
		return existing
	case existing == nil:
		return extra
	default:
		return joinedCounter{first: existing, second: extra}
	}
}

// Flow is one client facing flow. Byte totals are lock free, every other field is
// guarded by the registry lock.
type Flow struct {
	ID       uint64
	Network  string
	Source   string
	Local    string
	Inbound  string
	read     flowCounter
	write    flowCounter
	target   string
	outbound string
	detected string
	app      string
	exe      string
	pid      int
	own      bool
	lookup   string
	tries    int
	srcIP    xnet.Address
	srcPort  uint16
	dstIP    xnet.Address
	dstPort  uint16
	done     bool
	// sampler only
	startedAt  time.Time
	closedAt   time.Time
	upBps      int64
	downBps    int64
	prevUp     int64
	prevDown   int64
	prevSample time.Time
}

// Read returns the counter for traffic the application sent.
func (f *Flow) Read() stats.Counter {
	if f == nil {
		return nil
	}
	return &f.read
}

// Write returns the counter for traffic the application received.
func (f *Flow) Write() stats.Counter {
	if f == nil {
		return nil
	}
	return &f.write
}

var (
	enabled   atomic.Bool
	registry  sync.RWMutex
	flows     = make(map[uint64]*Flow)
	nextID    atomic.Uint64
	publishKV sync.Once
	startOne  sync.Once
)

type ctxKey struct{}

// Enable starts sampling. The metrics app calls it, so tracking cost is only paid
// when something can read the table.
func Enable() {
	if enabled.Swap(true) {
		return
	}
	publishKV.Do(func() {
		expvarPublish()
	})
	startOne.Do(func() {
		go sampleLoop()
	})
}

// New records a new flow. Returns nil when tracking is off or the table is full.
func New(network xnet.Network, source, local xnet.Destination, inboundTag string) *Flow {
	if !enabled.Load() || source.Address == nil || local.Address == nil {
		return nil
	}
	f := &Flow{
		Network:    network.SystemString(),
		Source:     hostPort(source.Address, source.Port),
		Local:      hostPort(local.Address, local.Port),
		Inbound:    inboundTag,
		srcIP:      source.Address,
		srcPort:    uint16(source.Port),
		startedAt:  time.Now(),
		prevSample: time.Now(),
	}
	if !source.Address.Family().IsIP() {
		f.done = true
		f.lookup = "no address"
	} else {
		// Resolve while the socket is guaranteed to still exist. A one shot UDP query is
		// gone well before the sampler reaches a lazily registered flow, which is what made
		// live rows read as unidentified.
		if owner, found := lookupOwner(f.Network, f.srcIP, f.srcPort, f.dstIP, f.dstPort); found {
			f.app = owner.name
			f.exe = owner.exe
			f.pid = owner.pid
			f.own = owner.pid > 0 && owner.pid == os.Getpid()
			f.done = true
		}
	}

	registry.Lock()
	if len(flows) >= maxLiveFlows {
		registry.Unlock()
		return nil
	}
	f.ID = nextID.Add(1)
	flows[f.ID] = f
	registry.Unlock()

	return f
}

// WithFlow carries the flow through the stages that annotate it.
func WithFlow(ctx context.Context, f *Flow) context.Context {
	if f == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKey{}, f)
}

// Wrap counts the bytes of a client facing connection.
func Wrap(conn stat.Connection, f *Flow) stat.Connection {
	if f == nil {
		return conn
	}
	return &stat.CounterConnection{
		Connection:   conn,
		ReadCounter:  f.Read(),
		WriteCounter: f.Write(),
	}
}

// UplinkCounter and DownlinkCounter are the variants for xray's UDP associations,
// which carry plain counters instead of a wrapped conn.
func UplinkCounter(existing stats.Counter, f *Flow) stats.Counter {
	return joinCounter(existing, f.Read())
}

func DownlinkCounter(existing stats.Counter, f *Flow) stats.Counter {
	return joinCounter(existing, f.Write())
}

// UpdateRoute annotates a flow once the dispatcher has picked target and detour.
func UpdateRoute(ctx context.Context, target xnet.Destination, outboundTag string) {
	f := fromContext(ctx)
	if f == nil {
		return
	}
	detected := ""
	if content := session.ContentFromContext(ctx); content != nil {
		detected = content.Protocol
	}

	registry.Lock()
	defer registry.Unlock()
	f.target = hostPort(target.Address, target.Port)
	f.dstIP = target.Address
	f.dstPort = uint16(target.Port)
	if outboundTag != "" {
		f.outbound = outboundTag
	}
	if detected != "" {
		f.detected = detected
	}
}

// Rebind moves a flow to the real socket endpoints, used by proxies that only learn
// the application's UDP socket after the first datagram.
func Rebind(ctx context.Context, source, local xnet.Destination) {
	f := fromContext(ctx)
	if f == nil || !source.IsValid() || source.Port == 0 {
		return
	}
	registry.Lock()
	defer registry.Unlock()
	f.Source = hostPort(source.Address, source.Port)
	f.Local = hostPort(local.Address, local.Port)
	f.srcIP = source.Address
	f.srcPort = uint16(source.Port)
	if f.app == "" {
		f.done = false
		f.lookup = ""
		f.tries = 0
	}
}

// Release marks the flow finished; it stays visible for a moment so a panel can
// show the last rate instead of dropping rows mid transfer.
func Release(f *Flow) {
	if f == nil {
		return
	}
	registry.Lock()
	f.closedAt = time.Now()
	registry.Unlock()
}

func fromContext(ctx context.Context) *Flow {
	if ctx == nil {
		return nil
	}
	f, _ := ctx.Value(ctxKey{}).(*Flow)
	return f
}

func hostPort(address xnet.Address, port xnet.Port) string {
	if address == nil {
		address = xnet.AnyIP
	}
	return stdnet.JoinHostPort(address.String(), port.String())
}

func sampleLoop() {
	ticker := time.NewTicker(sampleInterval)
	defer ticker.Stop()

	previous := time.Now()
	for now := range ticker.C {
		elapsed := now.Sub(previous)
		previous = now

		// owner resolution is cached inside common/net, so a burst of new flows costs
		// one table enumeration for the whole burst rather than one per flow

		registry.Lock()
		lookups := 0
		for id, f := range flows {
			up := f.read.Value()
			down := f.write.Value()
			window := now.Sub(f.prevSample)
			if window <= 0 {
				window = elapsed
			}
			if window > 0 {
				weight := 1 - math.Exp(-float64(window)/float64(rateTau))
				f.upBps += int64(float64(bytesPerSecond(up, f.prevUp, window)-f.upBps) * weight)
				f.downBps += int64(float64(bytesPerSecond(down, f.prevDown, window)-f.downBps) * weight)
			}
			f.prevUp, f.prevDown, f.prevSample = up, down, now

			if !f.closedAt.IsZero() {
				// the transfer is over; a decaying tail reads as traffic that is not there
				f.upBps, f.downBps = 0, 0
			}

			if !f.done && lookups < maxOwnerPerTick &&
				(f.tries < maxOwnerRetries || f.tries%ownerRetryStride == 0) {
				lookups++
				f.tries++
				if owner, found := lookupOwner(f.Network, f.srcIP, f.srcPort, f.dstIP, f.dstPort); found {
					f.app, f.exe, f.pid = owner.name, owner.exe, owner.pid
					f.own = owner.pid > 0 && owner.pid == os.Getpid()
					f.done = true
				} else if f.tries >= maxOwnerRetries {
					f.lookup = "not found"
					if !f.closedAt.IsZero() {
						// the transfer is over and the socket went with it
						f.done = true
					}
				}
			}

			if !f.closedAt.IsZero() && now.Sub(f.closedAt) > closedRetention {
				delete(flows, id)
			}
		}
		registry.Unlock()
	}
}

func bytesPerSecond(current, previous int64, elapsed time.Duration) int64 {
	if current < previous || elapsed <= 0 {
		return 0
	}
	return (current - previous) * int64(time.Second) / int64(elapsed)
}

// ownerInfo is one resolved application, filled by the platform lookup.
type ownerInfo struct {
	name string
	exe  string
	pid  int
	err  string
}
