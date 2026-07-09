// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type benchmarkMetrics struct {
	success        atomic.Int64
	errors         atomic.Int64
	totalLatencyNS atomic.Int64
	maxLatencyMS   int64
	lock           sync.Mutex
	histogram      map[int64]int64
}

type metricsSnapshot struct {
	success        int64
	errors         int64
	totalLatencyNS int64
	maxLatencyMS   int64
	histogram      map[int64]int64
}

func newBenchmarkMetrics(requestTimeout time.Duration) *benchmarkMetrics {
	return &benchmarkMetrics{
		maxLatencyMS: latencyCapMS(requestTimeout),
		histogram:    make(map[int64]int64),
	}
}

func (m *benchmarkMetrics) record(success bool, latency time.Duration) {
	if success {
		m.success.Add(1)
		m.totalLatencyNS.Add(latency.Nanoseconds())
		bucket := min(int64(latency/time.Millisecond), m.maxLatencyMS)
		m.lock.Lock()
		m.histogram[bucket]++
		m.lock.Unlock()
		return
	}
	m.errors.Add(1)
}

func (m *benchmarkMetrics) snapshot() metricsSnapshot {
	m.lock.Lock()
	defer m.lock.Unlock()
	hist := make(map[int64]int64, len(m.histogram))
	for bucket, count := range m.histogram {
		hist[bucket] = count
	}
	return metricsSnapshot{
		success:        m.success.Load(),
		errors:         m.errors.Load(),
		totalLatencyNS: m.totalLatencyNS.Load(),
		maxLatencyMS:   m.maxLatencyMS,
		histogram:      hist,
	}
}

func diffSnapshot(before, after metricsSnapshot) metricsSnapshot {
	hist := make(map[int64]int64)
	for bucket, afterCount := range after.histogram {
		if delta := afterCount - before.histogram[bucket]; delta > 0 {
			hist[bucket] = delta
		}
	}
	return metricsSnapshot{
		success:        after.success - before.success,
		errors:         after.errors - before.errors,
		totalLatencyNS: after.totalLatencyNS - before.totalLatencyNS,
		maxLatencyMS:   after.maxLatencyMS,
		histogram:      hist,
	}
}

func printResults(label string, duration time.Duration, snap metricsSnapshot) {
	total := snap.success + snap.errors
	seconds := duration.Seconds()
	tps := 0.0
	if seconds > 0 {
		tps = float64(total) / seconds
	}
	avgMS := 0.0
	if snap.success > 0 {
		avgMS = float64(snap.totalLatencyNS) / 1_000_000.0 / float64(snap.success)
	}
	p50 := percentile(snap.histogram, snap.success, snap.maxLatencyMS, 0.50)
	p95 := percentile(snap.histogram, snap.success, snap.maxLatencyMS, 0.95)
	p99 := percentile(snap.histogram, snap.success, snap.maxLatencyMS, 0.99)
	maxLatency := maximumLatency(snap.histogram, snap.success)

	if label == "Monitor" {
		fmt.Printf("%s duration=%.3fs trxs=%d succ=%d err=%d tps=%.3f avg_ms=%.3f p50=%d p95=%d p99=%d max=%d\n",
			label, seconds, total, snap.success, snap.errors, tps, avgMS, p50, p95, p99, maxLatency)
		return
	}

	fmt.Printf("======================================================================\n")
	fmt.Printf("%s results:\n", label)
	fmt.Printf("duration=%.3fs trxs=%d succ=%d err=%d tps=%.3f avg_ms=%.3f p50=%d p95=%d p99=%d max=%d\n",
		seconds, total, snap.success, snap.errors, tps, avgMS, p50, p95, p99, maxLatency)
	fmt.Printf("======================================================================\n")
}

func percentile(hist map[int64]int64, successes int64, maxLatencyMS int64, pct float64) int64 {
	if successes <= 0 {
		return 0
	}
	target := int64(math.Ceil(float64(successes) * pct))
	buckets := make([]int64, 0, len(hist))
	for bucket := range hist {
		buckets = append(buckets, bucket)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })

	var cumulative int64
	for _, bucket := range buckets {
		cumulative += hist[bucket]
		if cumulative >= target {
			return bucket
		}
	}
	return maxLatencyMS
}

func maximumLatency(hist map[int64]int64, successes int64) int64 {
	if successes <= 0 {
		return 0
	}
	var maxBucket int64
	for bucket, count := range hist {
		if count > 0 && bucket > maxBucket {
			maxBucket = bucket
		}
	}
	return maxBucket
}

func latencyCapMS(requestTimeout time.Duration) int64 {
	if requestTimeout <= 0 {
		return math.MaxInt64
	}
	capMS := int64((requestTimeout + time.Millisecond - 1) / time.Millisecond)
	if capMS <= 0 {
		return 1
	}
	return capMS
}
