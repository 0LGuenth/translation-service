// Client-side load balancer for translation-llm.
//
// The llm Service is headless (clusterIP: None), so DNS `translation-llm`
// resolves to N pod IPs. Every 5s we re-resolve; between resolutions we pick
// a pod using power-of-two-choices over an in-flight counter (Mitzenmacher,
// 2001). Multiple Go replicas independently making these picks converge
// close-to-optimal without shared state.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// One llm pod. inflight is our local in-flight count (our view, not the pod's).
type llmPod struct {
	addr     string // "10.42.0.5:8000"
	inflight atomic.Int64
}

type router struct {
	host string // "translation-llm"
	port string // "8000"
	log  *slog.Logger

	mu       sync.RWMutex
	backends []*llmPod // current pod IPs, replaced wholesale on refresh
}

// newRouter parses a URL like http://translation-llm:8000 into host+port and
// kicks off the DNS refresh loop. Fails if the URL is malformed or the
// initial resolve returns nothing — the gateway can't function without a
// backend.
func newRouter(rawURL string, log *slog.Logger) (*router, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}
	host, port := u.Hostname(), u.Port()
	if port == "" {
		port = "80"
	}
	r := &router{host: host, port: port, log: log}
	if err := r.refresh(context.Background()); err != nil {
		return nil, fmt.Errorf("initial resolve %s: %w", host, err)
	}
	go r.loop()
	return r, nil
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

// refresh replaces the pod list in place. Existing counters for still-
// present pods are preserved so we don't lose the in-flight signal on every
// resolve.
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
	for _, ip := range ips {
		addr := net.JoinHostPort(ip, r.port)
		if b, ok := existing[addr]; ok {
			next = append(next, b)
		} else {
			next = append(next, &llmPod{addr: addr})
			r.log.Info("backend added", "addr", addr)
		}
	}
	// Log removals (present before, gone now).
	for addr := range existing {
		if _, still := findAddr(next, addr); !still {
			r.log.Info("backend removed", "addr", addr)
		}
	}
	r.backends = next
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

// pick returns a pod chosen by power-of-two: sample two at random, take the
// one with the lower in-flight count. Falls back to the only choice when
// there's one, and returns nil when there are zero (caller must handle).
func (r *router) pick() *llmPod {
	r.mu.RLock()
	bs := r.backends
	r.mu.RUnlock()
	switch len(bs) {
	case 0:
		return nil
	case 1:
		return bs[0]
	}
	a := bs[rand.IntN(len(bs))]
	c := bs[rand.IntN(len(bs))]
	if a.inflight.Load() <= c.inflight.Load() {
		return a
	}
	return c
}
