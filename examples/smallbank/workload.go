// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type benchmarker struct {
	client         smallBankInvoker
	config         *workloadConfig
	metrics        *benchmarkMetrics
	requestTimeout time.Duration
	requestSeq     atomic.Uint64
}

type smallBankInvoker interface {
	invoke(ctx context.Context, req request) (response, error)
}

func newBenchmarker(client smallBankInvoker, cfg *workloadConfig, requestTimeout time.Duration) *benchmarker {
	return &benchmarker{
		client:         client,
		config:         cfg,
		metrics:        newBenchmarkMetrics(requestTimeout),
		requestTimeout: requestTimeout,
	}
}

func (b *benchmarker) execute(startUnixMS int64) {
	if startUnixMS > 0 {
		waitUntilStartUnixMS(startUnixMS)
	}

	windows := b.buildPhaseSchedule()
	maxTerminals := 0
	for _, window := range windows {
		maxTerminals = max(maxTerminals, window.terminals)
	}

	var wg sync.WaitGroup
	startCh := make(chan struct{})
	phaseDone := make([]*sync.WaitGroup, len(windows))
	for i := range phaseDone {
		phaseDone[i] = &sync.WaitGroup{}
		phaseDone[i].Add(maxTerminals)
	}

	monitorStop := b.startMonitor()
	baseline := b.metrics.snapshot()
	workloadStart := time.Now()

	for terminalID := 0; terminalID < maxTerminals; terminalID++ {
		wg.Add(1)
		go func(terminalID int) {
			defer wg.Done()
			<-startCh
			for phaseIdx, window := range windows {
				if terminalID < window.terminals {
					b.runPhase(terminalID, window)
				} else {
					sleepUntil(window.end)
				}
				phaseDone[phaseIdx].Done()
			}
		}(terminalID)
	}

	close(startCh)
	phaseBaseline := baseline
	for i, done := range phaseDone {
		done.Wait()
		current := b.metrics.snapshot()
		printResults(fmt.Sprintf("Phase %d", i+1), windows[i].duration(), diffSnapshot(phaseBaseline, current))
		phaseBaseline = current
	}

	wg.Wait()
	if monitorStop != nil {
		close(monitorStop)
	}
	total := diffSnapshot(baseline, b.metrics.snapshot())
	printResults("Total", time.Since(workloadStart), total)
}

func (b *benchmarker) runPhase(terminalID int, window phaseWindow) {
	sleepUntil(window.start)

	saturate := window.rate == 0
	interval := time.Duration(0)
	if !saturate {
		interval = time.Duration(float64(time.Second) / window.rate)
		if interval <= 0 {
			interval = time.Nanosecond
		}
	}
	next := window.start

	for time.Now().Before(window.end) {
		if !saturate {
			sleepUntil(next)
		}

		req := b.nextRequest(terminalID)
		start := time.Now()
		resp, err := b.invoke(req)
		latency := time.Since(start)
		b.metrics.record(err == nil && resp.benchmarkSuccess(), latency)

		if !saturate {
			next = next.Add(interval)
			for next.Before(time.Now()) {
				next = next.Add(interval)
			}
		}
	}
}

func (b *benchmarker) nextRequest(terminalID int) request {
	seq := b.requestSeq.Add(1)

	return request{
		ClientID: fmt.Sprintf("terminal-%d", terminalID),
		ID:       fmt.Sprintf("%d", seq),
	}
}

func (b *benchmarker) invoke(req request) (response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.requestTimeout)
	defer cancel()
	return b.client.invoke(ctx, req)
}

func (b *benchmarker) startMonitor() chan struct{} {
	if b.config.MonitorIntervalSec <= 0 {
		return nil
	}
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Duration(b.config.MonitorIntervalSec) * time.Second)
		defer ticker.Stop()

		baseline := b.metrics.snapshot()
		lastSample := time.Now()
		for {
			select {
			case <-ticker.C:
				now := time.Now()
				current := b.metrics.snapshot()
				printResults("Monitor", now.Sub(lastSample), diffSnapshot(baseline, current))
				baseline = current
				lastSample = now
			case <-stop:
				return
			}
		}
	}()
	return stop
}

func (b *benchmarker) buildPhaseSchedule() []phaseWindow {
	windows := make([]phaseWindow, len(b.config.Phases))
	start := time.Now()
	for i, phase := range b.config.Phases {
		end := start.Add(time.Duration(phase.DurationSec) * time.Second)
		windows[i] = phaseWindow{
			start:     start,
			end:       end,
			rate:      phase.Rate,
			terminals: phase.Terminals,
		}
		start = end
	}
	return windows
}

type phaseWindow struct {
	start     time.Time
	end       time.Time
	rate      float64
	terminals int
}

func (p phaseWindow) duration() time.Duration {
	return p.end.Sub(p.start)
}

func sleepUntil(t time.Time) {
	for {
		remaining := time.Until(t)
		if remaining <= 0 {
			return
		}
		if remaining > 100*time.Millisecond {
			remaining = 100 * time.Millisecond
		}
		time.Sleep(remaining)
	}
}

func waitUntilStartUnixMS(startUnixMS int64) {
	target := time.UnixMilli(startUnixMS)
	fmt.Printf("Waiting until benchmark start unix ms %d (%s)\n", startUnixMS, target.Format(time.RFC3339Nano))
	sleepUntil(target)
}
