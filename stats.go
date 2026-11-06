package main

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const recentEventLimit = 30

// retryEvent describes one absorbed retry.
type retryEvent struct {
	Time    time.Time
	Model   string
	Attempt int
	Reason  string
	Wait    string
}

// modelCounters aggregates per-model activity.
type modelCounters struct {
	Wrapped   int64
	Retries   int64
	Exhausted int64
	Fatal     int64
}

// pluginStats tracks plugin activity for the status resource page.
type pluginStats struct {
	startedAt time.Time

	wrappedTotal  atomic.Int64
	wrappedStream atomic.Int64
	wrappedSync   atomic.Int64
	retries       atomic.Int64
	exhausted     atomic.Int64
	fatal         atomic.Int64

	mu     sync.Mutex
	models map[string]*modelCounters
	recent []retryEvent
}

var stats = &pluginStats{startedAt: time.Now(), models: map[string]*modelCounters{}}

func (s *pluginStats) modelEntryLocked(model string) *modelCounters {
	entry := s.models[model]
	if entry == nil {
		entry = &modelCounters{}
		s.models[model] = entry
	}
	return entry
}

func (s *pluginStats) recordWrapped(model string, stream bool) {
	s.wrappedTotal.Add(1)
	if stream {
		s.wrappedStream.Add(1)
	} else {
		s.wrappedSync.Add(1)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modelEntryLocked(model).Wrapped++
}

func (s *pluginStats) recordRetry(model string, attempt int, reason, wait string) {
	s.retries.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modelEntryLocked(model).Retries++
	s.recent = append(s.recent, retryEvent{
		Time:    time.Now(),
		Model:   model,
		Attempt: attempt,
		Reason:  reason,
		Wait:    wait,
	})
	if len(s.recent) > recentEventLimit {
		s.recent = append([]retryEvent(nil), s.recent[len(s.recent)-recentEventLimit:]...)
	}
}

func (s *pluginStats) recordExhausted(model string) {
	s.exhausted.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modelEntryLocked(model).Exhausted++
}

func (s *pluginStats) recordFatal(model string) {
	s.fatal.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modelEntryLocked(model).Fatal++
}

type statsSnapshot struct {
	StartedAt     time.Time
	Uptime        time.Duration
	WrappedTotal  int64
	WrappedStream int64
	WrappedSync   int64
	Retries       int64
	Exhausted     int64
	Fatal         int64
	Models        map[string]modelCounters
	Recent        []retryEvent
}

func (s *pluginStats) snapshot() statsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	models := make(map[string]modelCounters, len(s.models))
	for name, entry := range s.models {
		models[name] = *entry
	}
	recent := append([]retryEvent(nil), s.recent...)
	return statsSnapshot{
		StartedAt:     s.startedAt,
		Uptime:        time.Since(s.startedAt),
		WrappedTotal:  s.wrappedTotal.Load(),
		WrappedStream: s.wrappedStream.Load(),
		WrappedSync:   s.wrappedSync.Load(),
		Retries:       s.retries.Load(),
		Exhausted:     s.exhausted.Load(),
		Fatal:         s.fatal.Load(),
		Models:        models,
		Recent:        recent,
	}
}

func (s *statsSnapshot) sortedModelNames() []string {
	names := make([]string, 0, len(s.Models))
	for name := range s.Models {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
