// Client-side load balancer for translation-llm over the headless Service.
// Hot pairs (HOT_PAIRS) balance across all pods; cold pairs pin to a K-pod
// consistent-hash shortlist. Final pick is power-of-two-choices on in-flight.
// Pod set re-resolved from DNS every 5s.
package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ringVNodes is virtual nodes per pod on the hash ring (even key spread).
const ringVNodes = 100

// shortlistK is how many pods a cold (affinity) pair pins to; 2 gives p2c a choice.
const shortlistK = 2

// One llm pod. inflight is our local in-flight count (our view, not the pod's).
type llmPod struct {
	addr     string // "10.42.0.5:8000"
	inflight atomic.Int64
}

// ringEntry is one virtual node: a hash position and the pod that owns it.
type ringEntry struct {
	hash uint64
	pod  *llmPod
}

type router struct {
	host string // "translation-llm"
	port string // "8000"
	log  *slog.Logger
	hot  map[string]bool // pairs available on every pod (src-tgt, lowercased)

	mu       sync.RWMutex
	backends []*llmPod   // current pod IPs, replaced wholesale on refresh
	ring     []ringEntry // consistent-hash ring over backends, sorted by hash
}

// newRouter parses the llm URL, seeds backends, and starts the DNS refresh
// loop. hotPairs is the raw HOT_PAIRS env (comma-separated src-tgt). Fails if
// the URL is bad or the initial resolve finds no backends.
func newRouter(rawURL, hotPairs string, log *slog.Logger) (*router, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}
	host, port := u.Hostname(), u.Port()
	if port == "" {
		port = "80"
	}
	r := &router{host: host, port: port, log: log, hot: parseHotPairs(hotPairs)}
	if err := r.refresh(context.Background()); err != nil {
		return nil, fmt.Errorf("initial resolve %s: %w", host, err)
	}
	go r.loop()
	return r, nil
}

// parseHotPairs turns "de-en, en-de" into {"de-en":true,"en-de":true}.
func parseHotPairs(raw string) map[string]bool {
	hot := map[string]bool{}
	for _, spec := range strings.Split(raw, ",") {
		spec = strings.ToLower(strings.TrimSpace(spec))
		if spec == "" {
			continue
		}
		src, tgt, ok := strings.Cut(spec, "-")
		if !ok || src == "" || tgt == "" {
			continue
		}
		hot[src+"-"+tgt] = true
	}
	return hot
}

func (r *router) loop() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for range t.C {
		if err := r.refresh(context.Background()); err != nil {
			r.log.Warn("dns refresh failed", "host", r.host, "err", err)
		}
	}
}

// refresh re-resolves the pod list, preserving counters for surviving pods and
// rebuilding the ring only when the pod set changes.
func (r *router) refresh(ctx context.Context) error {
	ips, err := net.DefaultResolver.LookupHost(ctx, r.host)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	existing := make(map[string]*llmPod, len(r.backends))
	for _, b := range r.backends {
		existing[b.addr] = b
	}
	next := make([]*llmPod, 0, len(ips))
	changed := false
	for _, ip := range ips {
		addr := net.JoinHostPort(ip, r.port)
		if b, ok := existing[addr]; ok {
			next = append(next, b)
		} else {
			next = append(next, &llmPod{addr: addr})
			r.log.Info("backend added", "addr", addr)
			changed = true
		}
	}
	// Log removals (present before, gone now).
	for addr := range existing {
		if _, still := findAddr(next, addr); !still {
			r.log.Info("backend removed", "addr", addr)
			changed = true
		}
	}
	r.backends = next
	if changed || r.ring == nil {
		r.ring = buildRing(next)
	}
	return nil
}

func findAddr(bs []*llmPod, addr string) (*llmPod, bool) {
	for _, b := range bs {
		if b.addr == addr {
			return b, true
		}
	}
	return nil, false
}

// buildRing places ringVNodes fnv-1a virtual nodes per pod, sorted for binary
// search. Deterministic: a pair maps to a stable shortlist per pod set.
func buildRing(pods []*llmPod) []ringEntry {
	ring := make([]ringEntry, 0, len(pods)*ringVNodes)
	for _, p := range pods {
		for i := 0; i < ringVNodes; i++ {
			ring = append(ring, ringEntry{hash: hashKey(p.addr + "#" + strconv.Itoa(i)), pod: p})
		}
	}
	sort.Slice(ring, func(i, j int) bool { return ring[i].hash < ring[j].hash })
	return ring
}

func hashKey(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// shortlist returns up to K distinct pods for pair, walking the ring clockwise
// from the pair's hash position.
func (r *router) shortlist(pair string, k int) []*llmPod {
	r.mu.RLock()
	ring := r.ring
	r.mu.RUnlock()
	if len(ring) == 0 {
		return nil
	}
	h := hashKey(pair)
	start := sort.Search(len(ring), func(i int) bool { return ring[i].hash >= h })
	out := make([]*llmPod, 0, k)
	seen := make(map[*llmPod]bool, k)
	for i := 0; i < len(ring) && len(out) < k; i++ {
		p := ring[(start+i)%len(ring)].pod
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

// pick chooses a pod for (src,tgt) and returns the routing reason
// ("hot"/"affinity"). Returns nil when there are no backends.
func (r *router) pick(src, tgt string) (*llmPod, string) {
	pair := strings.ToLower(src) + "-" + strings.ToLower(tgt)
	if r.hot[pair] {
		r.mu.RLock()
		bs := r.backends
		r.mu.RUnlock()
		return pickByLoad(bs), "hot"
	}
	return pickByLoad(r.shortlist(pair, shortlistK)), "affinity"
}

// backendAddrs returns a snapshot of the current backend addrs (host:port).
// Same locking as pick(); the returned slice is the caller's to keep.
func (r *router) backendAddrs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, len(r.backends))
	for i, b := range r.backends {
		out[i] = b.addr
	}
	return out
}

// pickAddr returns the addr pick(src,tgt) would route to, or "" if no backends.
func (r *router) pickAddr(src, tgt string) string {
	pod, _ := r.pick(src, tgt)
	if pod == nil {
		return ""
	}
	return pod.addr
}

// pickByLoad is power-of-two-choices: the lower-inflight of two random
// candidates. Falls back to the sole/zero candidate.
func pickByLoad(candidates []*llmPod) *llmPod {
	switch len(candidates) {
	case 0:
		return nil
	case 1:
		return candidates[0]
	}
	a := candidates[rand.IntN(len(candidates))]
	c := candidates[rand.IntN(len(candidates))]
	if a.inflight.Load() <= c.inflight.Load() {
		return a
	}
	return c
}
