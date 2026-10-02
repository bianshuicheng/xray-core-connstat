package tun

import (
	"context"
	"expvar"
	stdlibnet "net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	c "github.com/xtls/xray-core/common/ctx"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/flowwatch"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
)

// Handler is managing object that tie together tun interface, ip stack and dispatch connections to the routing
type Handler struct {
	ctx             context.Context
	config          *Config
	stack           Stack
	tun             Tun
	policyManager   policy.Manager
	dispatcher      routing.Dispatcher
	tag             string
	sniffingRequest session.SniffingRequest
	uplinkCounter   stats.Counter
	downlinkCounter stats.Counter
	// tunIndex and offlineGate are set when the outbound interface binding is active; only
	// then can the admission gate below ask the same route question the dialer asks.
	tunIndex    int
	offlineGate bool
}

type tunUDPStatsWriter struct {
	writer  buf.Writer
	counter stats.Counter
}

func (w *tunUDPStatsWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	for len(mb) > 0 {
		remaining, packet := buf.SplitFirst(mb)
		packetSize := packet.Len()
		if err := w.writer.WriteMultiBuffer(buf.MultiBuffer{packet}); err != nil {
			buf.ReleaseMulti(remaining)
			return err
		}
		w.counter.Add(int64(packetSize))
		mb = remaining
	}
	return nil
}

// ConnectionHandler interface with the only method that stack is going to push new connections to
type ConnectionHandler interface {
	HandleConnection(conn net.Conn, destination net.Destination)
}

// Handler implements ConnectionHandler
var _ ConnectionHandler = (*Handler)(nil)

// Handler implements common.Runnable
var _ common.Runnable = (*Handler)(nil)

// Init the Handler instance with necessary parameters
func (t *Handler) Init(ctx context.Context, pm policy.Manager, dispatcher routing.Dispatcher) error {
	// Retrieve tag and sniffing config from context (set by AlwaysOnInboundHandler)
	if inbound := session.InboundFromContext(ctx); inbound != nil {
		t.tag = inbound.Tag
	}
	if content := session.ContentFromContext(ctx); content != nil {
		t.sniffingRequest = content.SniffingRequest
	}

	t.ctx = core.ToBackgroundDetachedContext(ctx)
	t.policyManager = pm
	t.dispatcher = dispatcher

	if len(t.tag) > 0 && pm.ForSystem().Stats.InboundUplink {
		statsManager := core.MustFromContext(ctx).GetFeature(stats.ManagerType()).(stats.Manager)
		name := "inbound>>>" + t.tag + ">>>traffic>>>uplink"
		c, _ := statsManager.GetOrRegisterCounter(name)
		if c != nil {
			t.uplinkCounter = c
		}
	}
	if len(t.tag) > 0 && pm.ForSystem().Stats.InboundDownlink {
		statsManager := core.MustFromContext(ctx).GetFeature(stats.ManagerType()).(stats.Manager)
		name := "inbound>>>" + t.tag + ">>>traffic>>>downlink"
		c, _ := statsManager.GetOrRegisterCounter(name)
		if c != nil {
			t.downlinkCounter = c
		}
	}

	return nil
}

func (t *Handler) Start() error {
	tunName := t.config.Name
	tunInterface, err := NewTun(t.config)
	if err != nil {
		return err
	}
	t.registerTunAddresses(tunInterface)

	if t.config.AutoOutboundsInterface != "" {
		tunIndex, err := tunInterface.Index()
		if err != nil {
			_ = tunInterface.Close()
			return err
		}
		t.tunIndex = tunIndex
		t.offlineGate = true
		if t.config.AutoOutboundsInterface == "auto" {
			t.config.AutoOutboundsInterface = ""
		}
		updater = &InterfaceUpdater{tunIndex: tunIndex, fixedName: t.config.AutoOutboundsInterface}
		updater.Update()
		internet.RegisterDialerController(func(network, address string, c syscall.RawConn) error {
			addrPort, _ := netip.ParseAddrPort(address)
			// skip loopback
			if addrPort.Addr().IsLoopback() || strings.HasPrefix(strings.ToLower(address), "localhost:") {
				return nil
			}
			// Ask the route table before connecting, not after: a dial whose destination would
			// leave through the TUN we serve has to fail here, otherwise the handshake is
			// answered by our own stack and the core ends up serving its own traffic.
			if egressIsTun(addrPort.Addr(), tunIndex) {
				routeRefused.Add(1)
				net.SetUplinkDown(true)
				tripBreaker()
				return errors.New("[tun] route to ", address, " leaves through our own TUN, refusing to dial")
			}
			iface := updater.Get()
			if iface == nil {
				// Refusing is the only safe answer here. An unbound dial follows the routing
				// table, and while the uplink is down that table points at the TUN we serve.
				uplinkRefused.Add(1)
				net.SetUplinkDown(true)
				tripBreaker()
				return errors.New("[tun] no outbound interface available, refusing to dial ", address)
			}
			err := c.Control(func(fd uintptr) {
				err := setinterface(network, address, fd, iface)
				if err != nil {
					errors.LogInfoInner(context.Background(), err, "[tun] falied to set interface")
				}
			})
			if err == nil {
				// A bound dial is the freshest possible evidence that egress works again;
				// consumers (DNS fast-fail) heal from this without any extra timer.
				net.SetUplinkDown(false)
			}
			return err
		})
	}

	errors.LogInfo(t.ctx, tunName, " created")

	tunStackOptions := StackOptions{
		Tun:         tunInterface,
		IdleTimeout: t.policyManager.ForLevel(t.config.UserLevel).Timeouts.ConnectionIdle,
	}
	tunStack, err := NewStack(t.ctx, tunStackOptions, t)
	if err != nil {
		_ = tunInterface.Close()
		return err
	}

	err = tunStack.Start()
	if err != nil {
		_ = tunStack.Close()
		_ = tunInterface.Close()
		return err
	}

	err = tunInterface.Start()
	if err != nil {
		_ = tunStack.Close()
		_ = tunInterface.Close()
		return err
	}

	// Platform-specific system DNS takeover, where the platform implements it.
	// Non-fatal: a failure leaves DNS management with the OS.
	if c, ok := tunInterface.(interface {
		ConfigureSystemDNS(context.Context, string) error
	}); ok {
		if err := c.ConfigureSystemDNS(t.ctx, t.tag); err != nil {
			errors.LogInfoInner(t.ctx, err, "[tun] system DNS not configured")
		}
	}

	t.stack = tunStack
	t.tun = tunInterface

	errors.LogInfo(t.ctx, tunName, " up")
	return nil
}

var (
	selfLoops    atomic.Int64
	selfLoopWarn atomic.Int64
	selfLoopVar  sync.Once
	routeRefused atomic.Int64
	// offlineRefused counts connections closed at admission because their destination
	// routes through our own TUN - the dial would be refused anyway. uplinkRefused counts
	// dials refused because no usable outbound interface exists at all.
	offlineRefused atomic.Int64
	offlineWarn    atomic.Int64
	uplinkRefused  atomic.Int64
	dispatchFailed atomic.Int64
	dispatchWarn   atomic.Int64
)

func publishSelfLoopCount() {
	expvar.Publish("tunloop", expvar.Func(func() any {
		return map[string]any{
			"dropped":         selfLoops.Load(),
			"reaped":          reaped.Load(),
			"over_cap":        overCap.Load(),
			"live":            liveConnections(),
			"self_refused":    net.SelfRefusedCount(),
			"route_refused":   routeRefused.Load(),
			"uplink_refused":  uplinkRefused.Load(),
			"offline_refused": offlineRefused.Load(),
			"breaker_refused": breakerRefused.Load(),
		}
	}))
}

// ownsSocket tells whether the TUN connection is one of this core's own dials that looped
// back. The dial side registers the endpoint the moment it connects, so unlike the OS socket
// table this has no blind window for a socket that just appeared.
func ownsSocket(network net.Network, source net.Destination) bool {
	name := "tcp"
	if network == net.Network_UDP {
		name = "udp"
	}
	return net.IsSelfOutbound(name, source.Address, uint16(source.Port))
}

// registerTunAddresses publishes the addresses of this TUN so outbound dials can tell
// whether they are leaving through the interface they are supposed to serve. The interface
// also owns link-local and auto-assigned addresses, which is where IPv6 dials come from.
func (t *Handler) registerTunAddresses(tun Tun) {
	collect := func(raw string) {
		if prefix, err := netip.ParsePrefix(raw); err == nil {
			net.RegisterTunAddress(net.ParseAddress(prefix.Addr().String()))
			return
		}
		if addr, err := netip.ParseAddr(raw); err == nil {
			net.RegisterTunAddress(net.ParseAddress(addr.String()))
		}
	}

	for _, raw := range t.config.Gateway {
		collect(raw)
	}

	index, err := tun.Index()
	if err != nil {
		return
	}
	ifc, err := stdlibnet.InterfaceByIndex(index)
	if err != nil {
		return
	}
	addrs, err := ifc.Addrs()
	if err != nil {
		return
	}
	for _, a := range addrs {
		collect(a.String())
	}
}

// HandleConnection pass the connection coming from the ip stack to the routing dispatcher
func (t *Handler) HandleConnection(conn net.Conn, destination net.Destination) {
	// when handling is done with any outcome, always signal back to the incoming connection
	// to close, send completion packets back to the network, and cleanup
	defer conn.Close()

	ctx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	ctx = c.ContextWithID(ctx, session.NewID())

	// if the connection is already closed, conn.RemoteAddr() will be nil
	// due to gvisor weird behavior
	remote := conn.RemoteAddr()
	if remote == nil {
		errors.LogInfo(t.ctx, "dropped quickly closed connection")
		return
	}
	source := net.DestinationFromAddr(remote)
	selfLoopVar.Do(publishSelfLoopCount)
	if ownsSocket(destination.Network, source) {
		// Serving this would dial it again: one stuck route becomes thousands of
		// connections the core holds with itself.
		dropped := selfLoops.Add(1)
		if now := time.Now().Unix(); now-selfLoopWarn.Load() > 30 {
			selfLoopWarn.Store(now)
			errors.LogWarning(t.ctx, "[tun] dropped a self dialled connection, ", dropped, " in total; the outbound interface is unusable")
		}
		return
	}
	isUDP := destination.Network == net.Network_UDP

	// When the uplink is gone the best route to almost every destination is our own TUN,
	// so each dial the dispatcher makes is refused and the connection sits here for the
	// zombie timeout before the reaper collects it. A reconnect storm then parks up to
	// maxLiveTun such conns, each holding a gvisor endpoint, goroutine stacks and buffers
	// - the memory climb seen while the cable is pulled. Closing at admission hands the
	// retry back to the app immediately. Addresses this core serves itself (the TUN's own,
	// loopback) and DNS need no outbound dial and stay open.
	if t.offlineGate && destination.Port != 53 && !net.IsTunAddress(destination.Address) {
		if dest, ok := netip.AddrFromSlice(destination.Address.IP()); ok && !dest.IsLoopback() &&
			egressIsTun(dest, t.tunIndex) {
			dropped := offlineRefused.Add(1)
			if now := time.Now().Unix(); now-offlineWarn.Load() > 30 {
				offlineWarn.Store(now)
				errors.LogWarning(t.ctx, "[tun] uplink routes through our own TUN, closing ", destination, " at admission, ", dropped, " in total")
			}
			return
		}
	}

	if !isUDP && (t.uplinkCounter != nil || t.downlinkCounter != nil) {
		conn = &stat.CounterConnection{
			Connection:   conn,
			ReadCounter:  t.uplinkCounter,
			WriteCounter: t.downlinkCounter,
		}
	}

	conn, unwatch, admitted := watchDownstream(conn, isUDP)
	if !admitted {
		return
	}
	defer unwatch()

	flow := flowwatch.New(destination.Network, source, net.DestinationFromAddr(conn.LocalAddr()), t.tag)
	conn = flowwatch.Wrap(conn, flow)
	defer flowwatch.Release(flow)

	inbound := session.Inbound{
		Name:          "tun",
		Tag:           t.tag,
		CanSpliceCopy: 3,
		Source:        source,
		User: &protocol.MemoryUser{
			Level: t.config.UserLevel,
		},
	}

	ctx = session.ContextWithInbound(ctx, &inbound)
	ctx = flowwatch.WithFlow(ctx, flow)
	ctx = session.ContextWithContent(ctx, &session.Content{
		SniffingRequest: t.sniffingRequest,
	})
	ctx = session.SubContextFromMuxInbound(ctx)

	ctx = log.ContextWithAccessMessage(ctx, &log.AccessMessage{
		From:   inbound.Source,
		To:     destination,
		Status: log.AccessAccepted,
		Reason: "",
	})
	errors.LogInfo(ctx, "processing from ", source, " to ", destination)

	reader := &buf.TimeoutWrapperReader{Reader: buf.NewReader(conn)}
	writer := buf.NewWriter(conn)
	if isUDP {
		reader.Counter = t.uplinkCounter
		if t.downlinkCounter != nil {
			writer = &tunUDPStatsWriter{writer: writer, counter: t.downlinkCounter}
		}
	}

	link := &transport.Link{
		Reader: reader,
		Writer: writer,
	}
	if err := t.dispatcher.DispatchLink(ctx, destination, link); err != nil {
		// Every storm connection ends here; a line each would flood stdout and with it the
		// GUI that ingests it. First failure logs, then one summary line per 30s window.
		failed := dispatchFailed.Add(1)
		if now := time.Now().Unix(); now-dispatchWarn.Load() > 30 {
			dispatchWarn.Store(now)
			errors.LogError(t.ctx, errors.New("[tun] dispatch failed, ", failed, " in total so far").Base(err))
		}
	}
}

// Close implements common.Closable.
func (t *Handler) Close() error {
	return errors.Combine(common.CloseIfExists(t.stack), common.CloseIfExists(t.tun))
}

// Network implements proxy.Inbound
// and exists only to comply to proxy interface, declaring it doesn't listen on any network,
// making the process not open any port for this inbound (input will be network interface)
func (t *Handler) Network() []net.Network {
	return []net.Network{}
}

// Process implements proxy.Inbound
// and exists only to comply to proxy interface, which should never get any inputs due to no listening ports
func (t *Handler) Process(ctx context.Context, network net.Network, conn stat.Connection, dispatcher routing.Dispatcher) error {
	return nil
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		t := &Handler{config: config.(*Config)}
		err := core.RequireFeatures(ctx, func(pm policy.Manager, dispatcher routing.Dispatcher) error {
			return t.Init(ctx, pm, dispatcher)
		})
		return t, err
	}))
}
