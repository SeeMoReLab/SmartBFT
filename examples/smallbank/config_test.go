// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"os"
	"strings"
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
	if cfg.MonitorIntervalSec != 1 {
		t.Fatalf("MonitorIntervalSec = %d, want 1", cfg.MonitorIntervalSec)
	}
	if len(cfg.Phases) != 1 {
		t.Fatalf("len(Phases) = %d, want 1", len(cfg.Phases))
	}
	if cfg.Phases[0].Terminals != 4 || cfg.Phases[0].Rate != 5 {
		t.Fatalf("Phases[0] = %+v", cfg.Phases[0])
	}
}

func TestSmallBankStateApply(t *testing.T) {
	state := newSmallBankState()
	for _, id := range []string{"1", "2", "3"} {
		resp := state.apply(request{ClientID: "client", ID: id, Padding: "xxxx"})
		if resp != (response{ClientID: "client", ID: id, Status: statusSuccess}) {
			t.Fatalf("apply %s = %+v", id, resp)
		}
	}
	if state.lastExecuted["client"] != 3 {
		t.Fatalf("watermark = %d, want 3", state.lastExecuted["client"])
	}
}

func TestEncodedRequestIsExactlyRequestSize(t *testing.T) {
	for _, clientID := range []string{"terminal-0", "terminal-511", "create-0"} {
		for _, id := range []string{"1", "9", "10", "99999", "1000000", "4294967296", "18446744073709551615"} {
			raw, err := encodeRequest(request{ClientID: clientID, ID: id, Padding: "stale"})
			if err != nil {
				t.Fatalf("encode %s/%s: %v", clientID, id, err)
			}
			if len(raw) != requestSizeBytes {
				t.Fatalf("encoded %s/%s is %d bytes, want %d", clientID, id, len(raw), requestSizeBytes)
			}
			decoded, err := decodeRequest(raw)
			if err != nil {
				t.Fatalf("decode %s/%s: %v", clientID, id, err)
			}
			if decoded.ClientID != clientID || decoded.ID != id {
				t.Fatalf("round trip %s/%s = %+v", clientID, id, decoded)
			}
		}
	}
	if requestSizeBytes != 512 {
		t.Fatalf("requestSizeBytes = %d, want 512", requestSizeBytes)
	}
}

func TestEncodeRequestRejectsOversizedRequest(t *testing.T) {
	if _, err := encodeRequest(request{ClientID: strings.Repeat("c", requestSizeBytes), ID: "1"}); err == nil {
		t.Fatal("expected a request larger than the request size to be rejected")
	}
}

func TestPendingTrackerForgetsDecidedAndCompletedRequests(t *testing.T) {
	pending := newPendingTracker()
	decided := request{ClientID: "client", ID: "1"}
	duplicate := request{ClientID: "client", ID: "2"}

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
	if report.P90ConsensusLatencyMs != 30 || report.P95ConsensusLatencyMs != 30 {
		t.Fatalf("consensus latency p90 %f p95 %f, want 30/30",
			report.P90ConsensusLatencyMs, report.P95ConsensusLatencyMs)
	}
	if report.P50BatchSize != 2 || report.P90BatchSize != 3 || report.P95BatchSize != 3 {
		t.Fatalf("batch size p50 %f p90 %f p95 %f, want 2/3/3",
			report.P50BatchSize, report.P90BatchSize, report.P95BatchSize)
	}
	if report.TimeoutMs != 15 {
		t.Fatalf("TimeoutMs = %d, want 15", report.TimeoutMs)
	}
	if report.AvgInterCommitGapMs != 100 || report.P50InterCommitGapMs != 100 ||
		report.P90InterCommitGapMs != 100 || report.P95InterCommitGapMs != 100 {
		t.Fatalf("inter-commit gaps = avg %f p50 %f p90 %f p95 %f, want 100/100/100/100",
			report.AvgInterCommitGapMs, report.P50InterCommitGapMs, report.P90InterCommitGapMs, report.P95InterCommitGapMs)
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
		roundTrip.P95InterCommitGapMs != report.P95InterCommitGapMs ||
		roundTrip.P90ConsensusLatencyMs != report.P90ConsensusLatencyMs ||
		roundTrip.P90InterCommitGapMs != report.P90InterCommitGapMs ||
		roundTrip.P50BatchSize != report.P50BatchSize ||
		roundTrip.P90BatchSize != report.P90BatchSize {
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

func TestLearningRequiresAnchor(t *testing.T) {
	_, err := newLearningManager(learningOptions{Enabled: true, AgentTarget: "127.0.0.1:1"})
	if err == nil || !strings.Contains(err.Error(), "--learning-anchor-unix-ms") {
		t.Fatalf("err = %v, want missing --learning-anchor-unix-ms", err)
	}
}

func TestLearningAnchoredGridEpisodes(t *testing.T) {
	anchor := time.Now()
	manager := &learningManager{
		featureDuration: 10 * time.Second,
		replyWait:       2 * time.Second,
		warmupDuration:  3 * time.Second,
		rewardDuration:  5 * time.Second,
		episodeAnchor:   anchor,
		anchorUnixMS:    1_000_000,
	}

	if got := manager.wallClockEpisodeStart(3); !got.Equal(anchor.Add(28 * time.Second)) {
		t.Fatalf("episode 3 start = %s, want anchor+28s", got.Sub(anchor))
	}
	if got := manager.gridApplyUnixMS(3); got != 1_040_000 {
		t.Fatalf("episode 3 grid apply = %d, want 1040000", got)
	}
	cases := []struct {
		now  time.Time
		want uint32
	}{
		{anchor.Add(-time.Minute), 1},
		{anchor.Add(-12 * time.Second), 1},
		{anchor.Add(-12*time.Second + time.Nanosecond), 2},
		{anchor.Add(8 * time.Second), 2},
		{anchor.Add(100 * time.Second), 7},
	}
	for _, tc := range cases {
		if got := manager.firstWallClockEpisodeAfter(tc.now); got != tc.want {
			t.Fatalf("first episode at anchor%+s = %d, want %d", tc.now.Sub(anchor), got, tc.want)
		}
	}
}

func TestLearningAnchoredOverrunSkipsToNextGridSlot(t *testing.T) {
	now := time.Now()
	manager := &learningManager{
		enabled:         true,
		metrics:         newLearningWindowMetrics(),
		currentEpisode:  2,
		featureDuration: 10 * time.Second,
		replyWait:       2 * time.Second,
		warmupDuration:  3 * time.Second,
		rewardDuration:  5 * time.Second,
		episodeAnchor:   now.Add(-25 * time.Second),
		wallClockStage:  wallClockLearningFeature,
	}

	next, ok := manager.handleWallClockEpisodeStart(2, manager.wallClockEpisodeStart(2))
	if !ok {
		t.Fatalf("episode start was not handled")
	}
	if next != 3 || manager.currentEpisode != 3 {
		t.Fatalf("next = %d current = %d, want 3", next, manager.currentEpisode)
	}
	if manager.wallClockStage != wallClockLearningIdle {
		t.Fatalf("stage = %d, want idle", manager.wallClockStage)
	}
}

func TestLearningAnchoredLoopStartsOnGridWithoutDelivery(t *testing.T) {
	manager := &learningManager{
		enabled:         true,
		metrics:         newLearningWindowMetrics(),
		featureDuration: 100 * time.Millisecond,
		replyWait:       50 * time.Millisecond,
		warmupDuration:  50 * time.Millisecond,
		rewardDuration:  100 * time.Millisecond,
		pollInterval:    time.Millisecond,
		rpcTimeout:      time.Millisecond,
		currentTimeout:  800 * time.Millisecond,
		lastTimeout:     800 * time.Millisecond,
		wallClockStage:  wallClockLearningIdle,
	}
	defer manager.close()

	// Episode 1 started 400 ms ago and episode 2 100 ms ago, so the loop
	// joins at episode 3, which starts in 200 ms.
	now := time.Now()
	manager.episodeAnchor = now.Add(manager.applyOffset() - 400*time.Millisecond)
	first := manager.firstWallClockEpisodeAfter(now)
	if first != 3 {
		t.Fatalf("first episode = %d, want 3", first)
	}
	manager.start()
	manager.lock.Lock()
	started := manager.currentEpisode
	manager.lock.Unlock()
	if started != first {
		t.Fatalf("started episode = %d, want %d", started, first)
	}

	// A delivery before the grid start must not start an episode.
	manager.recordConsensus(learningSample{Sequence: 1, DecisionTime: time.Now(), Latencies: []time.Duration{time.Millisecond}})
	manager.lock.Lock()
	stage := manager.wallClockStage
	manager.lock.Unlock()
	if stage != wallClockLearningIdle {
		t.Fatalf("stage after early delivery = %d, want idle", stage)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		manager.lock.Lock()
		pending := manager.pendingReward
		episode := manager.currentEpisode
		manager.lock.Unlock()
		if pending != nil {
			if pending.episode != first || episode != first+1 {
				t.Fatalf("reward episode = %d current = %d, want %d and %d", pending.episode, episode, first, first+1)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("anchored loop did not complete an episode")
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
