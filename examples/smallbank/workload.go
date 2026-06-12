// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type benchmarker struct {
	cluster        *cluster
	config         *workloadConfig
	metrics        *benchmarkMetrics
	requestTimeout time.Duration
	requestSeq     atomic.Uint64
}

func newBenchmarker(c *cluster, cfg *workloadConfig, requestTimeout time.Duration) *benchmarker {
	return &benchmarker{
		cluster:        c,
		config:         cfg,
		metrics:        newBenchmarkMetrics(),
		requestTimeout: requestTimeout,
	}
}

func (b *benchmarker) createAccounts(workers int) error {
	if workers <= 0 {
		workers = 1
	}
	fmt.Printf("Creating %d accounts with %d workers...\n", b.config.NumAccounts, workers)
	start := time.Now()

	jobs := make(chan uint64, workers)
	var wg sync.WaitGroup
	var firstErr atomic.Value

	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for accountID := range jobs {
				req := request{
					ClientID:             fmt.Sprintf("create-%d", workerID),
					ID:                   fmt.Sprintf("%d", accountID),
					Type:                 txCreateAccount,
					CustomerID:           accountID,
					CustomerName:         fmt.Sprintf("Customer%010d", accountID),
					SavingsBalanceCents:  1_000_000,
					CheckingBalanceCents: 1_000_000,
				}
				resp, err := b.invoke(req)
				if err != nil {
					firstErr.CompareAndSwap(nil, err)
					continue
				}
				if resp.Status != statusSuccess {
					firstErr.CompareAndSwap(nil, fmt.Errorf("create account %d failed: %s", accountID, resp.Error))
				}
			}
		}(workerID)
	}

	for id := uint64(0); id < b.config.NumAccounts; id++ {
		jobs <- id
	}
	close(jobs)
	wg.Wait()

	if val := firstErr.Load(); val != nil {
		return val.(error)
	}
	fmt.Printf("Account creation finished in %.3fs\n", time.Since(start).Seconds())
	return nil
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
					b.runPhase(terminalID, phaseIdx, window)
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

func (b *benchmarker) runPhase(terminalID int, phaseIdx int, window phaseWindow) {
	sleepUntil(window.start)

	seed := b.terminalSeed(terminalID, phaseIdx)
	rnd := rand.New(rand.NewSource(seed))
	accountSeed := int64(uint64(seed) ^ uint64(0x9E3779B97F4A7C15))
	accountRnd := rand.New(rand.NewSource(accountSeed))

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

		req := b.nextRequest(terminalID, rnd, accountRnd, window.weights)
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

func (b *benchmarker) nextRequest(terminalID int, rnd *rand.Rand, accountRnd *rand.Rand, weights [6]int) request {
	customerID := uint64(accountRnd.Int63n(int64(b.config.NumAccounts)))
	destID := uint64(accountRnd.Int63n(int64(b.config.NumAccounts)))
	for destID == customerID {
		destID = uint64(accountRnd.Int63n(int64(b.config.NumAccounts)))
	}
	amount := int64(100 + rnd.Intn(9901))
	tx := selectTxType(rnd, weights)
	seq := b.requestSeq.Add(1)

	return request{
		ClientID:       fmt.Sprintf("terminal-%d", terminalID),
		ID:             fmt.Sprintf("%d", seq),
		Type:           tx,
		CustomerID:     customerID,
		DestCustomerID: destID,
		AmountCents:    amount,
	}
}

func (b *benchmarker) invoke(req request) (response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.requestTimeout)
	defer cancel()
	return b.cluster.invoke(ctx, req)
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

func (b *benchmarker) terminalSeed(terminalID, phaseIdx int) int64 {
	return (b.config.RandomSeed*31 + int64(terminalID)*17) ^ (int64(phaseIdx) * 1003)
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
			weights:   phase.Weights,
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
	weights   [6]int
}

func (p phaseWindow) duration() time.Duration {
	return p.end.Sub(p.start)
}

func selectTxType(rnd *rand.Rand, weights [6]int) txType {
	value := rnd.Intn(100)
	cumulative := 0
	for i, weight := range weights {
		cumulative += weight
		if value < cumulative {
			return weightedTxTypes[i]
		}
	}
	return txWriteCheck
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
