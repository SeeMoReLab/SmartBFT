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
	latencies            []time.Duration
	batchSizes           []int
	totalTransactions    uint64
	totalConsensus       uint64
	leaderChangeCount    uint64
	regencyChangeCount   uint64
	previousLeader       uint64
	previousRegency      uint64
	havePreviousLeader   bool
	havePreviousRegency  bool
	firstDecisionTime    time.Time
	lastDecisionTime     time.Time
	throughputStartTime  time.Time
	previousDecisionTime time.Time
	interCommitGaps      []time.Duration
	timeout              time.Duration
	viewChangeCount      uint64
	noProgressViewChange uint64
}

type learningSample struct {
	Sequence     uint64
	View         uint64
	LeaderID     uint64
	BatchSize    int
	DecisionTime time.Time
	Latencies    []time.Duration
	Timeout      time.Duration
}

type throughputCalculation struct {
	totalTransactions uint64
	duration          time.Duration
	throughput        float32
}

func newLearningWindowMetrics() *learningWindowMetrics {
	return &learningWindowMetrics{}
}

func (m *learningWindowMetrics) reset() {
	*m = learningWindowMetrics{}
}

func (m *learningWindowMetrics) resetWithThroughputStart(start time.Time) {
	m.reset()
	m.throughputStartTime = start
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
	if !m.previousDecisionTime.IsZero() && sample.DecisionTime.After(m.previousDecisionTime) {
		m.interCommitGaps = append(m.interCommitGaps, sample.DecisionTime.Sub(m.previousDecisionTime))
	}
	m.previousDecisionTime = sample.DecisionTime
	m.lastDecisionTime = sample.DecisionTime

	if sample.Timeout > 0 {
		m.timeout = sample.Timeout
	}
	for _, latency := range sample.Latencies {
		if latency < 0 {
			continue
		}
		m.latencies = append(m.latencies, latency)
	}
}

func (m *learningWindowMetrics) recordViewChange() {
	m.viewChangeCount++
}

func (m *learningWindowMetrics) recordNoProgressViewChange() {
	m.noProgressViewChange++
}

func (m *learningWindowMetrics) buildReport() *adaptivetimers.PbftReport {
	return m.buildReportUntil(time.Time{}, false)
}

func (m *learningWindowMetrics) buildReportUntil(end time.Time, allowEmpty bool) *adaptivetimers.PbftReport {
	if !allowEmpty && len(m.latencies) == 0 {
		return nil
	}

	throughput := m.calculateThroughputUntil(end)

	avgBatchSize := float32(0)
	if m.totalConsensus > 0 {
		avgBatchSize = float32(float64(m.totalTransactions) / float64(m.totalConsensus))
	}

	return &adaptivetimers.PbftReport{
		TotalTransactions:         saturatingUint32(m.totalTransactions),
		TotalConsensusInstances:   saturatingUint32(m.totalConsensus),
		AvgConsensusLatencyMs:     avgDurationMS(m.latencies),
		P50ConsensusLatencyMs:     percentileDurationMS(m.latencies, 0.50),
		P95ConsensusLatencyMs:     percentileDurationMS(m.latencies, 0.95),
		ThroughputTps:             throughput.throughput,
		AvgBatchSize:              avgBatchSize,
		P95BatchSize:              percentileInt(m.batchSizes, 0.95),
		LeaderChangeCount:         saturatingUint32(m.leaderChangeCount),
		RegencyChangeCount:        saturatingUint32(m.regencyChangeCount),
		TimeoutMs:                 saturatingUint32(uint64(max(int64(0), m.timeout.Milliseconds()))),
		AvgInterCommitGapMs:       avgDurationMS(m.interCommitGaps),
		P50InterCommitGapMs:       percentileDurationMS(m.interCommitGaps, 0.50),
		P95InterCommitGapMs:       percentileDurationMS(m.interCommitGaps, 0.95),
		ViewChangeCount:           saturatingUint32(m.viewChangeCount),
		NoProgressViewChangeCount: saturatingUint32(m.noProgressViewChange),
	}
}

func (m *learningWindowMetrics) calculateThroughput() throughputCalculation {
	return m.calculateThroughputUntil(time.Time{})
}

func (m *learningWindowMetrics) calculateThroughputUntil(end time.Time) throughputCalculation {
	throughputStart := m.firstDecisionTime
	if !m.throughputStartTime.IsZero() {
		throughputStart = m.throughputStartTime
	}
	throughputEnd := m.lastDecisionTime
	if !end.IsZero() {
		throughputEnd = end
	}
	duration := throughputEnd.Sub(throughputStart)
	throughput := float32(0)
	if duration > 0 {
		throughput = float32(float64(m.totalTransactions) / duration.Seconds())
	}
	return throughputCalculation{
		totalTransactions: m.totalTransactions,
		duration:          duration,
		throughput:        throughput,
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
