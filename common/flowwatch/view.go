package flowwatch

import (
	"expvar"
	"sort"
	"time"
)

type flowView struct {
	ID           uint64 `json:"id"`
	Network      string `json:"net"`
	App          string `json:"app"`
	PID          int    `json:"pid,omitempty"`
	Exe          string `json:"exe,omitempty"`
	Lookup       string `json:"lookup,omitempty"`
	Self         bool   `json:"self,omitempty"`
	Source       string `json:"src"`
	Local        string `json:"local"`
	Inbound      string `json:"inbound,omitempty"`
	Outbound     string `json:"outbound,omitempty"`
	Target       string `json:"target,omitempty"`
	Detected     string `json:"detected,omitempty"`
	Uplink       int64  `json:"uplink"`
	Downlink     int64  `json:"downlink"`
	UplinkRate   int64  `json:"upBps"`
	DownlinkRate int64  `json:"downBps"`
	AgeSec       int64  `json:"ageSec"`
	Closing      bool   `json:"closing,omitempty"`
}

type totalsView struct {
	Flows        int   `json:"flows"`
	Uplink       int64 `json:"uplink"`
	Downlink     int64 `json:"downlink"`
	UplinkRate   int64 `json:"upBps"`
	DownlinkRate int64 `json:"downBps"`
}

type flowTableView struct {
	UpdatedMs int64      `json:"updatedMs"`
	TickMs    int64      `json:"tickMs"`
	Totals    totalsView `json:"totals"`
	Flows     []flowView `json:"flows"`
}

type appView struct {
	App          string   `json:"app"`
	PIDs         []int    `json:"pids,omitempty"`
	Exe          string   `json:"exe,omitempty"`
	Flows        int      `json:"flows"`
	Active       int      `json:"active"`
	Unresolved   int      `json:"unresolved,omitempty"`
	Uplink       int64    `json:"uplink"`
	Downlink     int64    `json:"downlink"`
	UplinkRate   int64    `json:"upBps"`
	DownlinkRate int64    `json:"downBps"`
	Targets      []string `json:"targets,omitempty"`
	TargetCount  int      `json:"targetCount"`
	Self         bool     `json:"self,omitempty"`
}

type appTableView struct {
	UpdatedMs int64      `json:"updatedMs"`
	TickMs    int64      `json:"tickMs"`
	Totals    totalsView `json:"totals"`
	Apps      []appView  `json:"apps"`
}

func expvarPublish() {
	expvar.Publish("flowwatch", expvar.Func(flowVars))
	expvar.Publish("appwatch", expvar.Func(appVars))
}

func flowVars() any {
	now := time.Now()
	view := flowTableView{UpdatedMs: now.UnixMilli(), TickMs: sampleInterval.Milliseconds()}

	registry.RLock()
	view.Flows = make([]flowView, 0, len(flows))
	for _, f := range flows {
		view.Flows = append(view.Flows, f.view(now))
		view.Totals.Flows++
		view.Totals.Uplink += f.read.Value()
		view.Totals.Downlink += f.write.Value()
		view.Totals.UplinkRate += f.upBps
		view.Totals.DownlinkRate += f.downBps
	}
	registry.RUnlock()

	sort.Slice(view.Flows, func(i, j int) bool {
		a, b := view.Flows[i], view.Flows[j]
		if a.UplinkRate+a.DownlinkRate != b.UplinkRate+b.DownlinkRate {
			return a.UplinkRate+a.DownlinkRate > b.UplinkRate+b.DownlinkRate
		}
		return a.Uplink+a.Downlink > b.Uplink+b.Downlink
	})
	return view
}

func appVars() any {
	now := time.Now()
	view := appTableView{UpdatedMs: now.UnixMilli(), TickMs: sampleInterval.Milliseconds()}

	type aggregate struct {
		app     appView
		pids    map[int]bool
		targets map[string]bool
	}
	byApp := make(map[string]*aggregate)

	registry.RLock()
	for _, f := range flows {
		// an unresolved application stays empty here; naming it is the panel's job
		item := byApp[f.app]
		if item == nil {
			item = &aggregate{
				app:     appView{App: f.app, Exe: f.exe, PIDs: []int{}},
				pids:    make(map[int]bool),
				targets: make(map[string]bool),
			}
			byApp[f.app] = item
		}
		if f.exe != "" && item.app.Exe == "" {
			item.app.Exe = f.exe
		}
		if f.pid > 0 {
			item.pids[f.pid] = true
		}
		if f.app == "" {
			item.app.Unresolved++
		}
		if f.own {
			item.app.Self = true
		}
		item.app.Flows++
		if f.closedAt.IsZero() {
			item.app.Active++
		}
		item.app.Uplink += f.read.Value()
		item.app.Downlink += f.write.Value()
		item.app.UplinkRate += f.upBps
		item.app.DownlinkRate += f.downBps
		if f.target != "" && !item.targets[f.target] {
			item.targets[f.target] = true
			item.app.TargetCount++
			if len(item.app.Targets) < maxTargetListPerApp {
				item.app.Targets = append(item.app.Targets, f.target)
			}
		}

		view.Totals.Flows++
		view.Totals.Uplink += f.read.Value()
		view.Totals.Downlink += f.write.Value()
		view.Totals.UplinkRate += f.upBps
		view.Totals.DownlinkRate += f.downBps
	}
	registry.RUnlock()

	view.Apps = make([]appView, 0, len(byApp))
	for _, item := range byApp {
		for pid := range item.pids {
			item.app.PIDs = append(item.app.PIDs, pid)
		}
		view.Apps = append(view.Apps, item.app)
	}
	sort.Slice(view.Apps, func(i, j int) bool {
		a, b := view.Apps[i], view.Apps[j]
		if a.UplinkRate+a.DownlinkRate != b.UplinkRate+b.DownlinkRate {
			return a.UplinkRate+a.DownlinkRate > b.UplinkRate+b.DownlinkRate
		}
		return a.Uplink+a.Downlink > b.Uplink+b.Downlink
	})
	return view
}

func (f *Flow) view(now time.Time) flowView {
	return flowView{
		ID:           f.ID,
		Network:      f.Network,
		App:          f.app,
		PID:          f.pid,
		Exe:          f.exe,
		Lookup:       f.lookup,
		Self:         f.own,
		Source:       f.Source,
		Local:        f.Local,
		Inbound:      f.Inbound,
		Outbound:     f.outbound,
		Target:       f.target,
		Detected:     f.detected,
		Uplink:       f.read.Value(),
		Downlink:     f.write.Value(),
		UplinkRate:   f.upBps,
		DownlinkRate: f.downBps,
		AgeSec:       int64(now.Sub(f.startedAt) / time.Second),
		Closing:      !f.closedAt.IsZero(),
	}
}
