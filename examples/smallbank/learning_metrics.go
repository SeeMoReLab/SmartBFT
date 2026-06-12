// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"math"
	"sort"
	"time"

	adaptivetimers "github.com/hyperledger-labs/SmartBFT/proto/adaptive_timers"
)

type learningWindowMetrics struct {
	latencies             []time.Duration
	batchSizes            []int
	totalTransactions     uint64
	totalConsensus        uint64
	timeoutViolations     uint64
	leaderChangeCount     uint64
	regencyChangeCount    uint64
	previousLeader        uint64
	previousRegency       uint64
	havePreviousLeader    bool
	havePreviousRegency   bool
	firstDecisionTime     time.Time
	lastDecisionTime      time.Time
	postDecisionLatencies []time.Duration
}

type learningSample struct {
	Sequence     uint64
	View         uint64
	LeaderID     uint64
	BatchSize    int
	DecisionTime time.Time
	Latencies    []time.Duration
	PostDecision time.Duration
	Timeout      time.Duration
}

func newLearningWindowMetrics() *learningWindowMetrics {
	return &learningWindowMetrics{}
}

func (m *learningWindowMetrics) reset() {
	*m = learningWindowMetrics{}
}

func (m *learningWindowMetrics) record(sample learningSample) {
	m.totalConsensus++
	m.totalTransactions += uint64(max(0, sample.BatchSize))
	m.batchSizes = append(m.batchSizes, max(0, sample.BatchSize))

	if sample.LeaderID > 0 {
		if m.havePreviousLeader && m.previousLeader != sample.LeaderID {
			m.leaderChangeCount++
		}
		m.previousLeader = sample.LeaderID
		m.havePreviousLeader = true
	}
	if m.havePreviousRegency && m.previousRegency != sample.View {
		m.regencyChangeCount++
	}
	m.previousRegency = sample.View
	m.havePreviousRegency = true

	if m.firstDecisionTime.IsZero() {
		m.firstDecisionTime = sample.DecisionTime
	}
	m.lastDecisionTime = sample.DecisionTime

	if sample.PostDecision >= 0 {
		m.postDecisionLatencies = append(m.postDecisionLatencies, sample.PostDecision)
	}

	for _, latency := range sample.Latencies {
		if latency < 0 {
			continue
		}
		m.latencies = append(m.latencies, latency)
		if sample.Timeout > 0 && latency > sample.Timeout {
			m.timeoutViolations++
		}
	}
}

func (m *learningWindowMetrics) buildReport() *adaptivetimers.PbftReport {
	if len(m.latencies) == 0 {
		return nil
	}

	throughput := float32(0)
	duration := m.lastDecisionTime.Sub(m.firstDecisionTime)
	if duration > 0 {
		throughput = float32(float64(m.totalTransactions) / duration.Seconds())
	}

	timeoutViolationRate := float32(0)
	if len(m.latencies) > 0 {
		timeoutViolationRate = float32(float64(m.timeoutViolations) / float64(len(m.latencies)))
	}

	avgBatchSize := float32(0)
	if m.totalConsensus > 0 {
		avgBatchSize = float32(float64(m.totalTransactions) / float64(m.totalConsensus))
	}

	return &adaptivetimers.PbftReport{
		TotalTransactions:           saturatingUint32(m.totalTransactions),
		TotalConsensusInstances:     saturatingUint32(m.totalConsensus),
		AvgConsensusLatencyMs:       avgDurationMS(m.latencies),
		P95ConsensusLatencyMs:       percentileDurationMS(m.latencies, 0.95),
		P99ConsensusLatencyMs:       percentileDurationMS(m.latencies, 0.99),
		ThroughputTps:               throughput,
		TimeoutViolationRate:        timeoutViolationRate,
		AvgBatchSize:                avgBatchSize,
		P95BatchSize:                percentileInt(m.batchSizes, 0.95),
		LeaderChangeCount:           saturatingUint32(m.leaderChangeCount),
		RegencyChangeCount:          saturatingUint32(m.regencyChangeCount),
		PhaseProposeAvgDelayMs:      0,
		PhaseProposeP95DelayMs:      0,
		PhaseWriteAvgDelayMs:        0,
		PhaseWriteP95DelayMs:        0,
		PhaseAcceptAvgDelayMs:       0,
		PhaseAcceptP95DelayMs:       0,
		PhasePostDecisionAvgDelayMs: avgDurationMS(m.postDecisionLatencies),
		PhasePostDecisionP95DelayMs: percentileDurationMS(m.postDecisionLatencies, 0.95),
	}
}

func avgDurationMS(values []time.Duration) float32 {
	if len(values) == 0 {
		return 0
	}
	var total time.Duration
	for _, value := range values {
		total += value
	}
	return float32(float64(total) / float64(len(values)) / float64(time.Millisecond))
}

func percentileDurationMS(values []time.Duration, percentile float64) float32 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := percentileIndex(len(sorted), percentile)
	return float32(float64(sorted[index]) / float64(time.Millisecond))
}

func percentileInt(values []int, percentile float64) float32 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]int(nil), values...)
	sort.Ints(sorted)
	return float32(sorted[percentileIndex(len(sorted), percentile)])
}

func percentileIndex(length int, percentile float64) int {
	if length <= 1 {
		return 0
	}
	target := int(math.Ceil(percentile*float64(length))) - 1
	if target < 0 {
		return 0
	}
	if target >= length {
		return length - 1
	}
	return target
}

func saturatingUint32(value uint64) uint32 {
	if value > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(value)
}
