package metrics

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"expvar"
	stdnet "net"
	"net/http"
	"net/http/pprof"
	"sort"
	"strconv"
	"strings"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/connstat"
	"github.com/xtls/xray-core/common/errors"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/signal/done"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	"github.com/xtls/xray-core/features/outbound"
	feature_stats "github.com/xtls/xray-core/features/stats"
)

type MetricsHandler struct {
	ohm          outbound.Manager
	statsManager feature_stats.Manager
	ctx          context.Context
	tag          string
	listen       string
	tcpListener  xnet.Listener
	listener     *OutboundListener
}

// NewMetricsHandler creates a new MetricsHandler based on the given config.
func NewMetricsHandler(ctx context.Context, config *Config) (*MetricsHandler, error) {
	c := &MetricsHandler{
		ctx:    ctx,
		tag:    config.Tag,
		listen: config.Listen,
	}
	common.Must(core.RequireFeatures(ctx, func(om outbound.Manager, sm feature_stats.Manager) {
		c.statsManager = sm
		c.ohm = om
	}))
	return c, nil
}

func (p *MetricsHandler) Type() interface{} {
	return (*MetricsHandler)(nil)
}

func (p *MetricsHandler) Start() error {
	handler := p.httpHandler()

	// direct listen a port if listen is set
	if p.listen != "" {
		TCPlistener, err := xnet.Listen("tcp", p.listen)
		if err != nil {
			return err
		}
		p.tcpListener = TCPlistener
		errors.LogInfo(context.Background(), "Metrics server listening on ", p.listen)

		go p.serve(TCPlistener, handler)
	}

	if p.tag == "" {
		if p.tcpListener == nil {
			return errors.New("metrics must have a tag or listen address")
		}
		return nil
	}

	listener := &OutboundListener{
		buffer: make(chan xnet.Conn, 4),
		done:   done.New(),
	}
	p.listener = listener

	go p.serve(listener, handler)

	if err := p.ohm.RemoveHandler(context.Background(), p.tag); err != nil {
		errors.LogInfo(context.Background(), "failed to remove existing handler")
	}

	if err := p.ohm.AddHandler(context.Background(), &Outbound{
		tag:      p.tag,
		listener: listener,
	}); err != nil {
		if closeErr := p.Close(); closeErr != nil {
			errors.LogErrorInner(context.Background(), closeErr, "failed to close metrics server after start failure")
		}
		return err
	}

	return nil
}

func (p *MetricsHandler) Close() error {
	var errs []error
	if p.tcpListener != nil {
		errs = append(errs, p.tcpListener.Close())
		p.tcpListener = nil
	}
	if p.listener != nil {
		errs = append(errs, p.listener.Close())
		p.listener = nil
	}
	if p.ohm != nil && p.tag != "" {
		if err := p.ohm.RemoveHandler(context.Background(), p.tag); err != nil {
			errors.LogInfo(context.Background(), "failed to remove metrics handler")
		}
	}
	return errors.Combine(errs...)
}

func (p *MetricsHandler) serve(listener xnet.Listener, handler http.Handler) {
	if err := http.Serve(listener, handler); err != nil && !isClosedListenerError(err) {
		errors.LogErrorInner(context.Background(), err, "failed to start metrics server")
	}
}

func isClosedListenerError(err error) bool {
	if err == nil {
		return true
	}
	if stderrors.Is(err, stdnet.ErrClosed) || stderrors.Is(err, http.ErrServerClosed) {
		return true
	}
	errText := err.Error()
	return strings.Contains(errText, "listen closed") ||
		strings.Contains(errText, "use of closed network connection")
}

func (p *MetricsHandler) httpHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/vars", p.handleDebugVars)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

func (p *MetricsHandler) handleDebugVars(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	vars := map[string]json.RawMessage{}
	expvar.Do(func(kv expvar.KeyValue) {
		value := json.RawMessage(kv.Value.String())
		if !json.Valid(value) {
			value = json.RawMessage("null")
		}
		vars[kv.Key] = value
	})
	vars["stats"] = marshalJSON(p.stats())
	vars["connstat"] = marshalJSON(p.connStats())
	vars["observatory"] = marshalJSON(p.observatoryStatus())

	payload, err := json.Marshal(vars)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(payload)
}

func marshalJSON(value interface{}) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage("null")
	}
	return data
}

// ConnStatInfo is the per-connection traffic entry published under the
// "connstat" key of /debug/vars by the connstat patch
// (see common/connstat and app/dispatcher/connstat.go).
type ConnStatInfo struct {
	ID       int64  `json:"id"`
	Dest     string `json:"dest"`
	Inbound  string `json:"inbound"`
	Outbound string `json:"outbound"`
	Process  string `json:"process,omitempty"`
	PID      int    `json:"pid,omitempty"`
	Path     string `json:"path,omitempty"`
	Uplink   int64  `json:"uplink"`
	Downlink int64  `json:"downlink"`
}

// connStats collects the live per-connection counters registered by the
// dispatcher: conn>>><id>|<dest>|<inboundTag>|<outboundTag>>>uplink/downlink.
func (p *MetricsHandler) connStats() []*ConnStatInfo {
	m := map[int64]*ConnStatInfo{}
	p.statsManager.VisitCounters(func(name string, counter feature_stats.Counter) bool {
		rest, ok := strings.CutPrefix(name, "conn>>>")
		if !ok {
			return true
		}
		meta, dir, ok := strings.Cut(rest, ">>>")
		if !ok {
			return true
		}
		parts := strings.Split(meta, "|")
		if len(parts) != 4 {
			return true
		}
		id, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return true
		}
		info, found := m[id]
		if !found {
			info = &ConnStatInfo{ID: id, Dest: parts[1], Inbound: parts[2], Outbound: parts[3]}
			m[id] = info
		}
		if pi, ok := connstat.ProcessOf(id); ok {
			info.Process, info.PID, info.Path = pi.Name, pi.PID, pi.Path
		}
		switch dir {
		case "uplink":
			info.Uplink = counter.Value()
		case "downlink":
			info.Downlink = counter.Value()
		}
		return true
	})
	out := make([]*ConnStatInfo, 0, len(m))
	for _, info := range m {
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	return out
}

func (p *MetricsHandler) stats() map[string]map[string]map[string]int64 {
	resp := map[string]map[string]map[string]int64{
		"inbound":  {},
		"outbound": {},
		"user":     {},
	}
	p.statsManager.VisitCounters(func(name string, counter feature_stats.Counter) bool {
		nameSplit := strings.Split(name, ">>>")
		if len(nameSplit) < 4 {
			return true
		}
		typeName, tagOrUser, direction := nameSplit[0], nameSplit[1], nameSplit[3]
		items, found := resp[typeName]
		if !found {
			items = map[string]map[string]int64{}
			resp[typeName] = items
		}
		if item, found := items[tagOrUser]; found {
			item[direction] = counter.Value()
		} else {
			items[tagOrUser] = map[string]int64{
				direction: counter.Value(),
			}
		}
		return true
	})
	return resp
}

func (p *MetricsHandler) observatoryStatus() interface{} {
	feature := core.MustFromContext(p.ctx).GetFeature(extension.ObservatoryType())
	if feature == nil {
		return nil
	}
	observatoryFeature := feature.(extension.Observatory)
	resp := map[string]*observatory.OutboundStatus{}
	if o, err := observatoryFeature.GetObservation(context.Background()); err != nil {
		return err
	} else {
		for _, x := range o.(*observatory.ObservationResult).GetStatus() {
			resp[x.OutboundTag] = x
		}
	}
	return resp
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, cfg interface{}) (interface{}, error) {
		return NewMetricsHandler(ctx, cfg.(*Config))
	}))
}
