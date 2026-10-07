// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"os"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	adaptivetimers "github.com/hyperledger-labs/SmartBFT/proto/adaptive_timers"
)

func TestLoadWorkloadConfig(t *testing.T) {
	cfg, err := loadWorkloadConfig("testdata/smallbank.xml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.NumAccounts != 20 {
		t.Fatalf("NumAccounts = %d, want 20", cfg.NumAccounts)
	}
	if cfg.MonitorIntervalSec != 1 {
		t.Fatalf("MonitorIntervalSec = %d, want 1", cfg.MonitorIntervalSec)
	}
	if len(cfg.Phases) != 1 {
		t.Fatalf("len(Phases) = %d, want 1", len(cfg.Phases))
	}
	if cfg.Phases[0].Weights != [6]int{15, 15, 25, 15, 15, 15} {
		t.Fatalf("Weights = %v", cfg.Phases[0].Weights)
	}
}

func TestSmallBankStateApply(t *testing.T) {
	state := newSmallBankState()
	create := request{
		ClientID:             "client",
		ID:                   "1",
		Type:                 txCreateAccount,
		CustomerID:           1,
		CustomerName:         "Alice",
		SavingsBalanceCents:  1000,
		CheckingBalanceCents: 2000,
	}
	if resp := state.apply(create); resp.Status != statusSuccess {
		t.Fatalf("create status = %s", resp.Status)
	}
	if resp := state.apply(request{ClientID: "client", ID: "2", Type: txDepositChecking, CustomerID: 1, AmountCents: 500}); resp.Status != statusSuccess {
		t.Fatalf("deposit status = %s", resp.Status)
	}
	balance := state.apply(request{ClientID: "client", ID: "3", Type: txBalance, CustomerID: 1})
	if balance.CheckingBalanceCents != 2500 || balance.SavingsBalanceCents != 1000 {
		t.Fatalf("balance = checking %d savings %d", balance.CheckingBalanceCents, balance.SavingsBalanceCents)
	}
}

func TestAccountCreationWorkersDefaultsToFirstPhaseTerminals(t *testing.T) {
	cfg := &workloadConfig{
		Phases: []phaseConfig{
			{Terminals: 7},
			{Terminals: 11},
		},
	}
	if workers := accountCreationWorkers(0, cfg); workers != 7 {
		t.Fatalf("workers = %d, want 7", workers)
	}
	if workers := accountCreationWorkers(-1, cfg); workers != 7 {
		t.Fatalf("workers = %d, want 7", workers)
	}
	if workers := accountCreationWorkers(3, cfg); workers != 3 {
		t.Fatalf("workers = %d, want 3", workers)
	}
}

func TestPendingTrackerForgetsDecidedAndCompletedRequests(t *testing.T) {
	pending := newPendingTracker()
	decided := request{ClientID: "client", ID: "1", Type: txBalance}
	duplicate := request{ClientID: "client", ID: "2", Type: txBalance}

	pending.markSubmitted(decided)
	pending.markSubmitted(duplicate)

	if _, ok := pending.latencyFor(decided, time.Now()); !ok {
		t.Fatalf("latency for a submitted request was not found")
	}
	if _, ok := pending.latencyFor(decided, time.Now()); ok {
		t.Fatalf("latency entry was kept after the request was decided")
	}

	pending.complete(response{ClientID: duplicate.ClientID, ID: duplicate.ID, Status: statusDuplicate})
	if _, ok := pending.latencyFor(duplicate, time.Now()); ok {
		t.Fatalf("latency entry was kept after the request completed")
	}
	if n := len(pending.submitted); n != 0 {
		t.Fatalf("submitted entries = %d, want 0", n)
	}
}

func TestProposalDelayController(t *testing.T) {
	ctrl, err := loadProposalDelayController("testdata/failure_spec.xml", time.Now().Add(-500*time.Millisecond).UnixMilli())
	if err != nil {
		t.Fatalf("load failure spec: %v", err)
	}

	// Failure spec IDs are 0-based, while SmartBFT node IDs are 1-based.
	// With leader node 1 and f=1 for a 4-node cluster, the leader token targets replica 0 / node 1.
	ctrl.observeLeader(1, []uint64{1, 2, 3, 4})
	delay := ctrl.delayForProposal(1)
	if delay != 25*time.Millisecond {
		t.Fatalf("leader delay = %s, want 25ms", delay)
	}

	delay = ctrl.delayForProposal(2)
	if delay != 0 {
		t.Fatalf("non-leader delay = %s, want 0", delay)
	}
}

func TestProposalDelayControllerPinsLeaderWindowPerPhase(t *testing.T) {
	ctrl, err := loadProposalDelayController("testdata/failure_spec.xml", time.Now().Add(-500*time.Millisecond).UnixMilli())
	if err != nil {
		t.Fatalf("load failure spec: %v", err)
	}

	nodes := []uint64{1, 2, 3, 4}
	ctrl.observeLeader(2, nodes)

	delay := ctrl.delayForProposal(2)
	if delay != 25*time.Millisecond {
		t.Fatalf("pinned leader delay = %s, want 25ms", delay)
	}

	ctrl.observeLeader(3, nodes)
	delay = ctrl.delayForProposal(3)
	if delay != 0 {
		t.Fatalf("new leader delay = %s, want 0", delay)
	}

	delay = ctrl.delayForProposal(2)
	if delay != 25*time.Millisecond {
		t.Fatalf("original pinned leader delay = %s, want 25ms", delay)
	}
}

func TestProposalDelayControllerIntervalRepinsLeaderWindow(t *testing.T) {
	specPath := t.TempDir() + "/failure_spec.xml"
	spec := `<?xml version="1.0"?>
<failureSpec>
    <warmUpTime>0</warmUpTime>
    <phases>
        <phase>
            <atTime>0</atTime>
            <pbft>
                <proposalDelay>
                    <interval>1</interval>
                    <replicas>
                        <replica>
                            <id>leader</id>
                            <delayMs>25</delayMs>
                        </replica>
                    </replicas>
                </proposalDelay>
            </pbft>
        </phase>
        <phase>
            <atTime>2</atTime>
            <pbft>
                <proposalDelay>
                    <replicas/>
                </proposalDelay>
            </pbft>
        </phase>
    </phases>
</failureSpec>`
	if err := os.WriteFile(specPath, []byte(spec), 0o600); err != nil {
		t.Fatalf("write failure spec: %v", err)
	}

	ctrl, err := loadProposalDelayController(specPath, time.Now().Add(-500*time.Millisecond).UnixMilli())
	if err != nil {
		t.Fatalf("load failure spec: %v", err)
	}

	nodes := []uint64{1, 2, 3, 4}
	ctrl.observeLeader(2, nodes)

	delay := ctrl.delayForProposal(2)
	if delay != 25*time.Millisecond {
		t.Fatalf("interval leader delay = %s, want 25ms", delay)
	}

	delay = ctrl.delayForProposal(1)
	if delay != 0 {
		t.Fatalf("previous interval leader delay = %s, want 0", delay)
	}
}

func TestProposalDelayControllerOpenEndedIntervalRepinsLeaderWindow(t *testing.T) {
	specPath := t.TempDir() + "/failure_spec.xml"
	spec := `<?xml version="1.0"?>
<failureSpec>
    <warmUpTime>0</warmUpTime>
    <phases>
        <phase>
            <atTime>0</atTime>
            <pbft>
                <proposalDelay>
                    <interval>1</interval>
                    <replicas>
                        <replica>
                            <id>leader</id>
                            <delayMs>25</delayMs>
                        </replica>
                    </replicas>
                </proposalDelay>
            </pbft>
        </phase>
    </phases>
</failureSpec>`
	if err := os.WriteFile(specPath, []byte(spec), 0o600); err != nil {
		t.Fatalf("write failure spec: %v", err)
	}

	ctrl, err := loadProposalDelayController(specPath, time.Now().Add(-500*time.Millisecond).UnixMilli())
	if err != nil {
		t.Fatalf("load failure spec: %v", err)
	}

	nodes := []uint64{1, 2, 3, 4}
	ctrl.observeLeader(2, nodes)

	delay := ctrl.delayForProposal(2)
	if delay != 25*time.Millisecond {
		t.Fatalf("first interval leader delay = %s, want 25ms", delay)
	}

	ctrl.startUnixMS = time.Now().Add(-1500 * time.Millisecond).UnixMilli()
	ctrl.observeLeader(3, nodes)

	delay = ctrl.delayForProposal(2)
	if delay != 0 {
		t.Fatalf("previous interval leader delay = %s, want 0", delay)
	}
	delay = ctrl.delayForProposal(3)
	if delay != 25*time.Millisecond {
		t.Fatalf("second interval leader delay = %s, want 25ms", delay)
	}
}

func TestProposalDelayControllerExplicitReplica(t *testing.T) {
	ctrl, err := loadProposalDelayController("testdata/failure_spec.xml", time.Now().Add(-1500*time.Millisecond).UnixMilli())
	if err != nil {
		t.Fatalf("load failure spec: %v", err)
	}

	// Explicit replica 2 maps to SmartBFT node 3.
	delay := ctrl.delayForProposal(3)
	if delay != 75*time.Millisecond {
		t.Fatalf("explicit replica delay = %s, want 75ms", delay)
	}
}

func TestLearningMetricsBuildPBFTReport(t *testing.T) {
	metrics := newLearningWindowMetrics()
	start := time.Now()
	metrics.record(learningSample{
		Sequence:     10,
		View:         1,
		LeaderID:     1,
		BatchSize:    2,
		DecisionTime: start,
		Latencies:    []time.Duration{10 * time.Millisecond, 20 * time.Millisecond},
		Timeout:      15 * time.Millisecond,
	})
	metrics.record(learningSample{
		Sequence:     11,
		View:         1,
		LeaderID:     2,
		BatchSize:    3,
		DecisionTime: start.Add(100 * time.Millisecond),
		Latencies:    []time.Duration{30 * time.Millisecond},
		Timeout:      15 * time.Millisecond,
	})
	metrics.recordViewChange()

	report := metrics.buildReportUntil(time.Time{})
	if report == nil {
		t.Fatalf("report is nil")
	}
	if report.TotalTransactions != 5 {
		t.Fatalf("TotalTransactions = %d, want 5", report.TotalTransactions)
	}
	if report.TotalConsensusInstances != 2 {
		t.Fatalf("TotalConsensusInstances = %d, want 2", report.TotalConsensusInstances)
	}
	if report.LeaderChangeCount != 1 {
		t.Fatalf("LeaderChangeCount = %d, want 1", report.LeaderChangeCount)
	}
	if report.P50ConsensusLatencyMs != 20 {
		t.Fatalf("P50ConsensusLatencyMs = %f, want 20", report.P50ConsensusLatencyMs)
	}
	if report.TimeoutMs != 15 {
		t.Fatalf("TimeoutMs = %d, want 15", report.TimeoutMs)
	}
	if report.AvgInterCommitGapMs != 100 || report.P50InterCommitGapMs != 100 || report.P95InterCommitGapMs != 100 {
		t.Fatalf("inter-commit gaps = avg %f p50 %f p95 %f, want 100/100/100",
			report.AvgInterCommitGapMs, report.P50InterCommitGapMs, report.P95InterCommitGapMs)
	}
	if report.ViewChangeCount != 1 {
		t.Fatalf("ViewChangeCount = %d, want 1", report.ViewChangeCount)
	}
	encoded, err := proto.Marshal(report)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	roundTrip := &adaptivetimers.SmartbftReport{}
	if err := proto.Unmarshal(encoded, roundTrip); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	if roundTrip.P50ConsensusLatencyMs != report.P50ConsensusLatencyMs ||
		roundTrip.P95InterCommitGapMs != report.P95InterCommitGapMs {
		t.Fatalf("round-trip report lost new fields: got %#v want %#v", roundTrip, report)
	}
}

func TestLearningMetricsThroughputCanStartBeforeFirstDecision(t *testing.T) {
	metrics := newLearningWindowMetrics()
	start := time.Now()
	metrics.resetWithThroughputStart(start)
	metrics.record(learningSample{
		Sequence:     10,
		View:         1,
		LeaderID:     1,
		BatchSize:    2,
		DecisionTime: start.Add(10 * time.Second),
		Latencies:    []time.Duration{10 * time.Millisecond},
		Timeout:      15 * time.Millisecond,
	})
	metrics.record(learningSample{
		Sequence:     11,
		View:         1,
		LeaderID:     1,
		BatchSize:    3,
		DecisionTime: start.Add(20 * time.Second),
		Latencies:    []time.Duration{20 * time.Millisecond},
		Timeout:      15 * time.Millisecond,
	})

	report := metrics.buildReportUntil(time.Time{})
	if report == nil {
		t.Fatalf("report is nil")
	}
	if got := report.ThroughputTps; got < 0.249 || got > 0.251 {
		t.Fatalf("ThroughputTps = %f, want about 0.25", got)
	}
}

func TestLearningWallClockMetricsUseFullWindow(t *testing.T) {
	start := time.Unix(100, 0)
	metrics := newLearningWindowMetrics()
	metrics.resetWithThroughputStart(start)
	metrics.timeout = 800 * time.Millisecond
	metrics.record(learningSample{
		Sequence:     1,
		View:         0,
		LeaderID:     1,
		BatchSize:    40,
		DecisionTime: start.Add(time.Second),
		Latencies:    []time.Duration{10 * time.Millisecond},
		Timeout:      800 * time.Millisecond,
	})
	metrics.record(learningSample{
		Sequence:     2,
		View:         0,
		LeaderID:     1,
		BatchSize:    60,
		DecisionTime: start.Add(2 * time.Second),
		Latencies:    []time.Duration{20 * time.Millisecond},
		Timeout:      800 * time.Millisecond,
	})

	report := metrics.buildReportUntil(start.Add(5 * time.Second))
	if report == nil {
		t.Fatalf("report is nil")
	}
	if got := report.ThroughputTps; got < 19.999 || got > 20.001 {
		t.Fatalf("ThroughputTps = %f, want 20", got)
	}
}

func TestLearningWallClockMetricsAllowZeroCommits(t *testing.T) {
	start := time.Unix(100, 0)
	metrics := newLearningWindowMetrics()
	metrics.resetWithThroughputStart(start)
	metrics.timeout = 700 * time.Millisecond
	metrics.recordViewChange()

	report := metrics.buildReportUntil(start.Add(5 * time.Second))
	if report == nil {
		t.Fatalf("report is nil")
	}
	if report.TotalConsensusInstances != 0 || report.TotalTransactions != 0 {
		t.Fatalf("zero-commit report has consensus=%d transactions=%d",
			report.TotalConsensusInstances, report.TotalTransactions)
	}
	if report.ThroughputTps != 0 {
		t.Fatalf("ThroughputTps = %f, want 0", report.ThroughputTps)
	}
	if report.TimeoutMs != 700 {
		t.Fatalf("TimeoutMs = %d, want 700", report.TimeoutMs)
	}
	if report.ViewChangeCount != 1 {
		t.Fatalf("ViewChangeCount = %d, want 1", report.ViewChangeCount)
	}
}

func TestLearningWallClockAppliesDecisionAtReplyDeadline(t *testing.T) {
	var applied time.Duration
	manager := &learningManager{
		enabled:        true,
		metrics:        newLearningWindowMetrics(),
		currentEpisode: 1,
		wallClockStage: wallClockLearningReplyWait,
		warmupDuration: 3 * time.Second,
		currentTimeout: 300 * time.Millisecond,
		lastTimeout:    300 * time.Millisecond,
		pollerEpisode:  1,
		pollerDecision: &learningDecision{timeout: 800 * time.Millisecond},
		applyTimeout: func(timeout time.Duration) error {
			applied = timeout
			return nil
		},
	}
	deadline := time.Unix(100, 0)

	if !manager.handleWallClockApplyDeadline(1, deadline) {
		t.Fatalf("apply deadline was not handled")
	}
	if applied != 800*time.Millisecond {
		t.Fatalf("applied timeout = %s, want 800ms", applied)
	}
	if manager.lastTimeout != 800*time.Millisecond {
		t.Fatalf("last timeout = %s, want 800ms", manager.lastTimeout)
	}
	if manager.wallClockStage != wallClockLearningWarmup {
		t.Fatalf("stage = %d, want warmup", manager.wallClockStage)
	}
	if got := manager.wallClockDeadline; !got.Equal(deadline.Add(3 * time.Second)) {
		t.Fatalf("warmup deadline = %s, want %s", got, deadline.Add(3*time.Second))
	}
}

func TestLearningWallClockMissingDecisionUsesCurrentTimeout(t *testing.T) {
	manager := &learningManager{
		enabled:        true,
		metrics:        newLearningWindowMetrics(),
		currentEpisode: 1,
		wallClockStage: wallClockLearningReplyWait,
		warmupDuration: 3 * time.Second,
		currentTimeout: 800 * time.Millisecond,
		lastTimeout:    300 * time.Millisecond,
		pollerEpisode:  1,
	}

	if !manager.handleWallClockApplyDeadline(1, time.Unix(100, 0)) {
		t.Fatalf("apply deadline was not handled")
	}
	if manager.currentTimeout != 800*time.Millisecond {
		t.Fatalf("current timeout = %s, want 800ms", manager.currentTimeout)
	}
	if manager.lastTimeout != 800*time.Millisecond {
		t.Fatalf("last timeout = %s, want 800ms", manager.lastTimeout)
	}
}

func TestLearningWallClockCompletesZeroCommitRewardWithoutDeliveryWakeup(t *testing.T) {
	manager := &learningManager{
		enabled:         true,
		metrics:         newLearningWindowMetrics(),
		currentEpisode:  1,
		featureDuration: 20 * time.Millisecond,
		replyWait:       10 * time.Millisecond,
		warmupDuration:  10 * time.Millisecond,
		rewardDuration:  20 * time.Millisecond,
		pollInterval:    time.Millisecond,
		rpcTimeout:      time.Millisecond,
		currentTimeout:  800 * time.Millisecond,
		lastTimeout:     800 * time.Millisecond,
		wallClockStage:  wallClockLearningIdle,
	}
	defer manager.close()

	now := time.Now()
	manager.recordConsensus(learningSample{
		Sequence:     1,
		View:         0,
		LeaderID:     1,
		BatchSize:    1,
		DecisionTime: now,
		Latencies:    []time.Duration{time.Millisecond},
		Timeout:      manager.currentTimeoutValue(),
	})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.lock.Lock()
		pending := manager.pendingReward
		episode := manager.currentEpisode
		manager.lock.Unlock()
		if pending != nil {
			if episode != 2 {
				t.Fatalf("current episode = %d, want 2", episode)
			}
			if pending.report.TotalConsensusInstances != 0 {
				t.Fatalf("reward consensus instances = %d, want 0", pending.report.TotalConsensusInstances)
			}
			if pending.report.ThroughputTps != 0 {
				t.Fatalf("reward throughput = %f, want 0", pending.report.ThroughputTps)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("wall-clock reward did not complete without another consensus delivery")
}

func TestLearningApplyRecommendedTimeoutCallsCallback(t *testing.T) {
	var applied time.Duration
	manager := &learningManager{
		currentTimeout: 5 * time.Second,
		applyTimeout: func(timeout time.Duration) error {
			applied = timeout
			return nil
		},
	}

	if err := manager.applyRecommendedTimeoutLocked(400 * time.Millisecond); err != nil {
		t.Fatalf("apply timeout: %v", err)
	}
	if applied != 400*time.Millisecond {
		t.Fatalf("applied = %s, want 400ms", applied)
	}
	if manager.currentTimeout != 400*time.Millisecond {
		t.Fatalf("currentTimeout = %s, want 400ms", manager.currentTimeout)
	}
}
