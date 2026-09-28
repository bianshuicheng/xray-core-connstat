// connstat-view shows live per-connection traffic added by the connstat patch
// in xray-core (patched builds register counters named
// conn>>><id>|<dest>|<inbound>|<outbound>>>uplink/downlink).
//
// Two data sources (pick one):
//
//	-url http://127.0.0.1:10812   poll the xray "metrics" app (/debug/vars),
//	                              the way v2rayN 7.x runs its core
//	-api 127.0.0.1:62756          poll StatsService gRPC (needs an "api"
//	                              inbound in the xray config)
//
// Example: connstat-view.exe                     (defaults: v2rayN on 10812)
//
//	connstat-view.exe -url http://127.0.0.1:10812 -interval 1s
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xtls/xray-core/app/stats/command"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type entry struct {
	id      int64
	dest    string
	inTag   string
	out     string
	process string
	path    string
	up      int64
	down    int64
	first   time.Time
}

type fetcher func() ([]entry, error)

func main() {
	url := flag.String("url", "http://127.0.0.1:10812", "xray metrics /debug/vars base url (v2rayN 7.x default port 10812); empty disables")
	api := flag.String("api", "", "StatsService gRPC endpoint host:port (alternative to -url)")
	interval := flag.Duration("interval", time.Second, "refresh interval")
	hideTag := flag.String("hide-inbound", "api", "hide connections from this inbound tag (empty = show all)")
	flag.Parse()

	var fetch fetcher
	switch {
	case *url != "":
		fetch = httpFetcher(*url)
		fmt.Fprintln(os.Stderr, "polling metrics:", *url)
	case *api != "":
		fetch = grpcFetcher(*api)
		fmt.Fprintln(os.Stderr, "polling grpc api:", *api)
	default:
		fmt.Fprintln(os.Stderr, "nothing to poll: set -url or -api")
		os.Exit(1)
	}

	enableVT()

	entries := make(map[int64]*entry)
	prevUp := make(map[int64]int64)
	prevDown := make(map[int64]int64)

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for range ticker.C {
		list, err := fetch()
		if err != nil {
			fmt.Fprintln(os.Stderr, "poll:", err)
			continue
		}

		for _, e := range list {
			if *hideTag != "" && e.inTag == *hideTag {
				continue
			}
			cur := entries[e.id]
			if cur == nil {
				cur = &entry{id: e.id, first: time.Now()}
				entries[e.id] = cur
			}
			*cur = entry{e.id, e.dest, e.inTag, e.out, e.process, e.path, e.up, e.down, cur.first}
		}
		live := make(map[int64]bool, len(list))
		for _, e := range list {
			if *hideTag == "" || e.inTag != *hideTag {
				live[e.id] = true
			}
		}
		for id := range entries {
			if !live[id] {
				delete(entries, id)
				delete(prevUp, id)
				delete(prevDown, id)
			}
		}

		ids := make([]int64, 0, len(entries))
		for id := range entries {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] }) // newest first

		fmt.Print("\033[H\033[2J")
		fmt.Printf("xray connstat - %s - %d live connection(s) (ctrl+c to quit)\n\n",
			time.Now().Format("15:04:05"), len(ids))
		fmt.Printf("%7s  %-30s  %-14s  %-10s -> %-10s  %10s  %10s  %10s  %10s  %8s\n",
			"ID", "DESTINATION", "PROCESS", "INBOUND", "OUTBOUND", "DOWN/s", "UP/s", "TOTAL-D", "TOTAL-U", "AGE")
		for _, id := range ids {
			e := entries[id]
			fmt.Printf("%7d  %-30s  %-14s  %-10s -> %-10s  %9s/s  %9s/s  %10s  %10s  %8s\n",
				id, trunc(e.dest, 30), trunc(e.process, 14), trunc(e.inTag, 10), trunc(e.out, 10),
				human(e.down-prevDown[id]), human(e.up-prevUp[id]),
				human(e.down), human(e.up), time.Since(e.first).Round(time.Second))
			prevDown[id] = e.down
			prevUp[id] = e.up
		}
		fmt.Println()
	}
}

// ---- xray metrics app (/debug/vars, connstat key added by the patch) ----

func httpFetcher(base string) fetcher {
	client := &http.Client{Timeout: 3 * time.Second}
	return func() ([]entry, error) {
		resp, err := client.Get(strings.TrimRight(base, "/") + "/debug/vars")
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		var doc struct {
			Connstat []struct {
				ID       int64  `json:"id"`
				Dest     string `json:"dest"`
				Inbound  string `json:"inbound"`
				Outbound string `json:"outbound"`
				Process  string `json:"process"`
				Path     string `json:"path"`
				Uplink   int64  `json:"uplink"`
				Downlink int64  `json:"downlink"`
			} `json:"connstat"`
		}
		if err := json.Unmarshal(body, &doc); err != nil {
			return nil, fmt.Errorf("bad metrics payload: %w", err)
		}
		out := make([]entry, 0, len(doc.Connstat))
		for _, c := range doc.Connstat {
			out = append(out, entry{id: c.ID, dest: c.Dest, inTag: c.Inbound, out: c.Outbound, process: c.Process, path: c.Path, up: c.Uplink, down: c.Downlink})
		}
		return out, nil
	}
}

// ---- xray StatsService gRPC (config needs an "api" inbound) ----

func grpcFetcher(addr string) fetcher {
	g, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "grpc dial:", err)
		os.Exit(1)
	}
	client := command.NewStatsServiceClient(g)
	ctx := context.Background()
	return func() ([]entry, error) {
		resp, err := client.QueryStats(ctx, &command.QueryStatsRequest{Pattern: "conn>>>"})
		if err != nil {
			return nil, err
		}
		m := map[int64]*entry{}
		for _, s := range resp.Stat {
			rest, ok := strings.CutPrefix(s.Name, "conn>>>")
			if !ok {
				continue
			}
			meta, dir, ok := strings.Cut(rest, ">>>")
			if !ok || (dir != "uplink" && dir != "downlink") {
				continue
			}
			parts := strings.Split(meta, "|")
			if len(parts) != 4 {
				continue
			}
			id, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				continue
			}
			e := m[id]
			if e == nil {
				e = &entry{id: id, dest: parts[1], inTag: parts[2], out: parts[3]}
				m[id] = e
			}
			if dir == "uplink" {
				e.up = s.Value
			} else {
				e.down = s.Value
			}
		}
		out := make([]entry, 0, len(m))
		for _, e := range m {
			out = append(out, *e)
		}
		return out, nil
	}
}

// ---- helpers ----

func human(n int64) string {
	f := float64(n)
	switch {
	case f >= 1<<30:
		return fmt.Sprintf("%.2fG", f/(1<<30))
	case f >= 1<<20:
		return fmt.Sprintf("%.2fM", f/(1<<20))
	case f >= 1<<10:
		return fmt.Sprintf("%.2fK", f/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "~"
}
