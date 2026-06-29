// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
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

func TestPendingTrackerReplaysCompletedResponse(t *testing.T) {
	pending := newPendingTracker()
	req := request{ClientID: "client", ID: "1", Type: txBalance}
	want := response{ClientID: "client", ID: "1", Status: statusSuccess}

	pending.complete(want)
	respCh, cancel := pending.register(req)
	defer cancel()

	select {
	case got := <-respCh:
		if got != want {
			t.Fatalf("response = %#v, want %#v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for cached response")
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
	delay := ctrl.delayForProposal(1, 1, []uint64{1, 2, 3, 4})
	if delay != 25*time.Millisecond {
		t.Fatalf("leader delay = %s, want 25ms", delay)
	}

	delay = ctrl.delayForProposal(2, 1, []uint64{1, 2, 3, 4})
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

	delay := ctrl.delayForProposal(2, 2, nodes)
	if delay != 25*time.Millisecond {
		t.Fatalf("pinned leader delay = %s, want 25ms", delay)
	}

	ctrl.observeLeader(3, nodes)
	delay = ctrl.delayForProposal(3, 3, nodes)
	if delay != 0 {
		t.Fatalf("new leader delay = %s, want 0", delay)
	}

	delay = ctrl.delayForProposal(2, 3, nodes)
	if delay != 25*time.Millisecond {
		t.Fatalf("original pinned leader delay = %s, want 25ms", delay)
	}
}

func TestProposalDelayControllerExplicitReplica(t *testing.T) {
	ctrl, err := loadProposalDelayController("testdata/failure_spec.xml", time.Now().Add(-1500*time.Millisecond).UnixMilli())
	if err != nil {
		t.Fatalf("load failure spec: %v", err)
	}

	// Explicit replica 2 maps to SmartBFT node 3.
	delay := ctrl.delayForProposal(3, 1, []uint64{1, 2, 3, 4})
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
	metrics.recordNoProgressViewChange()

	report := metrics.buildReport()
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
	if report.NoProgressViewChangeCount != 1 {
		t.Fatalf("NoProgressViewChangeCount = %d, want 1", report.NoProgressViewChangeCount)
	}
	encoded, err := proto.Marshal(report)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	roundTrip := &adaptivetimers.PbftReport{}
	if err := proto.Unmarshal(encoded, roundTrip); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	if roundTrip.P50ConsensusLatencyMs != report.P50ConsensusLatencyMs ||
		roundTrip.P95InterCommitGapMs != report.P95InterCommitGapMs ||
		roundTrip.NoProgressViewChangeCount != report.NoProgressViewChangeCount {
		t.Fatalf("round-trip report lost new fields: got %#v want %#v", roundTrip, report)
	}
}

func TestLearningEpisodeWindow(t *testing.T) {
	window := newLearningEpisodeWindow(10, 30, 20)
	if window.applyTick != 40 {
		t.Fatalf("applyTick = %d, want 40", window.applyTick)
	}
	if window.rewardTick != 50 {
		t.Fatalf("rewardTick = %d, want 50", window.rewardTick)
	}
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

func TestRequestTimeoutBackoffGrowsOnNoProgressViewsUntilCommitThenDecays(t *testing.T) {
	backoff, err := newRequestTimeoutBackoff(700*time.Millisecond, requestTimeoutBackoffOptions{
		Enabled:    true,
		MaxTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("create backoff: %v", err)
	}

	assertState := func(multiplier int, effective time.Duration) {
		t.Helper()
		state := backoff.state()
		if state.Multiplier != multiplier {
			t.Fatalf("multiplier = %d, want %d", state.Multiplier, multiplier)
		}
		if state.EffectiveTimeout != effective {
			t.Fatalf("effective = %s, want %s", state.EffectiveTimeout, effective)
		}
	}

	update := backoff.onNoProgressViewChange(1)
	if update.Apply {
		t.Fatalf("first no-progress view should keep regular timeout")
	}
	assertState(1, 700*time.Millisecond)

	update = backoff.onNoProgressViewChange(2)
	if !update.Apply {
		t.Fatalf("second consecutive no-progress view should apply 2x")
	}
	assertState(2, 1400*time.Millisecond)

	backoff.onNoProgressViewChange(3)
	assertState(4, 2800*time.Millisecond)

	backoff.onNoProgressViewChange(4)
	assertState(8, 5600*time.Millisecond)

	update = backoff.onCommit(5, 10)
	if !update.Decayed {
		t.Fatalf("commit after no-progress views should report decay")
	}
	if update.Previous.Multiplier != 8 || update.State.Multiplier != 7 {
		t.Fatalf("decay multiplier = %d -> %d, want 8 -> 7", update.Previous.Multiplier, update.State.Multiplier)
	}
	assertState(7, 4900*time.Millisecond)

	backoff.onCommit(5, 11)
	assertState(6, 4200*time.Millisecond)

	backoff.onCommit(5, 12)
	assertState(5, 3500*time.Millisecond)

	update = backoff.onNoProgressViewChange(6)
	if update.Apply {
		t.Fatalf("first no-progress view after commit should keep current multiplier")
	}
	assertState(5, 3500*time.Millisecond)

	backoff.onNoProgressViewChange(7)
	assertState(10, 7*time.Second)
}

func TestRequestTimeoutBackoffIgnoresDuplicateNoProgressViews(t *testing.T) {
	backoff, err := newRequestTimeoutBackoff(700*time.Millisecond, requestTimeoutBackoffOptions{
		Enabled:    true,
		MaxTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("create backoff: %v", err)
	}

	backoff.onNoProgressViewChange(1)
	backoff.onNoProgressViewChange(1)
	if got := backoff.state().Multiplier; got != 1 {
		t.Fatalf("multiplier after duplicate target view 1 = %d, want 1", got)
	}

	backoff.onNoProgressViewChange(2)
	backoff.onNoProgressViewChange(2)
	if got := backoff.state().Multiplier; got != 2 {
		t.Fatalf("multiplier after target view 2 and duplicate = %d, want 2", got)
	}

	backoff.onNoProgressViewChange(3)
	if got := backoff.state().Multiplier; got != 4 {
		t.Fatalf("multiplier after target view 3 = %d, want 4", got)
	}
}

func TestRequestTimeoutBackoffCountsSkippedNoProgressViews(t *testing.T) {
	backoff, err := newRequestTimeoutBackoff(700*time.Millisecond, requestTimeoutBackoffOptions{
		Enabled:    true,
		MaxTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("create backoff: %v", err)
	}

	backoff.onNoProgressViewChange(3)
	state := backoff.state()
	if state.Multiplier != 4 {
		t.Fatalf("multiplier after jump to target view 3 = %d, want 4", state.Multiplier)
	}
	if state.EffectiveTimeout != 2800*time.Millisecond {
		t.Fatalf("effective = %s, want 2800ms", state.EffectiveTimeout)
	}
}

func TestRequestTimeoutBackoffRequestTimeoutDoesNotGrowMultiplier(t *testing.T) {
	backoff, err := newRequestTimeoutBackoff(700*time.Millisecond, requestTimeoutBackoffOptions{
		Enabled:    true,
		MaxTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("create backoff: %v", err)
	}

	backoff.onRequestTimeout(1)
	backoff.onRequestTimeout(2)
	if got := backoff.state().Multiplier; got != 1 {
		t.Fatalf("multiplier after request timeouts = %d, want 1", got)
	}
}

func TestRequestTimeoutBackoffPreservesEffectiveTimeoutOnBaseChange(t *testing.T) {
	backoff, err := newRequestTimeoutBackoff(700*time.Millisecond, requestTimeoutBackoffOptions{
		Enabled:    true,
		MaxTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("create backoff: %v", err)
	}

	backoff.onNoProgressViewChange(1)
	backoff.onNoProgressViewChange(2)
	backoff.onNoProgressViewChange(3)
	backoff.onNoProgressViewChange(4)

	update, err := backoff.setBaseTimeout(1900 * time.Millisecond)
	if err != nil {
		t.Fatalf("set base: %v", err)
	}
	if !update.Apply {
		t.Fatalf("base change should apply recomputed effective timeout")
	}
	if update.State.Multiplier != 3 {
		t.Fatalf("multiplier = %d, want 3", update.State.Multiplier)
	}
	if update.State.EffectiveTimeout != 5700*time.Millisecond {
		t.Fatalf("effective = %s, want 5700ms", update.State.EffectiveTimeout)
	}
	if update.State.EffectiveViewChangeTimeout != 5700*time.Millisecond {
		t.Fatalf("effective view change = %s, want 5700ms", update.State.EffectiveViewChangeTimeout)
	}
}

func TestRequestTimeoutBackoffClampsMultiplierToMaxTimeout(t *testing.T) {
	backoff, err := newRequestTimeoutBackoff(700*time.Millisecond, requestTimeoutBackoffOptions{
		Enabled:    true,
		MaxTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("create backoff: %v", err)
	}

	for view := uint64(1); view <= 10; view++ {
		backoff.onNoProgressViewChange(view)
	}

	state := backoff.state()
	if state.Multiplier != 14 {
		t.Fatalf("multiplier = %d, want 14", state.Multiplier)
	}
	if state.EffectiveTimeout != 9800*time.Millisecond {
		t.Fatalf("effective = %s, want 9800ms", state.EffectiveTimeout)
	}

	update, err := backoff.setBaseTimeout(1900 * time.Millisecond)
	if err != nil {
		t.Fatalf("set base: %v", err)
	}
	if update.State.Multiplier != 5 {
		t.Fatalf("multiplier after base change = %d, want 5", update.State.Multiplier)
	}
	if update.State.EffectiveTimeout != 9500*time.Millisecond {
		t.Fatalf("effective after base change = %s, want 9500ms", update.State.EffectiveTimeout)
	}
}

func TestRequestTimeoutBackoffViewChangeResendIntervalUsesCappedMultiplier(t *testing.T) {
	backoff, err := newRequestTimeoutBackoff(100*time.Millisecond, requestTimeoutBackoffOptions{
		Enabled:    true,
		MaxTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("create backoff: %v", err)
	}

	if got := backoff.state().EffectiveViewChangeResendInterval; got != 100*time.Millisecond {
		t.Fatalf("initial resend interval = %s, want 100ms", got)
	}

	backoff.onNoProgressViewChange(1)
	backoff.onNoProgressViewChange(2)
	backoff.onNoProgressViewChange(3)
	if got := backoff.state().EffectiveViewChangeResendInterval; got != 400*time.Millisecond {
		t.Fatalf("resend interval after multiplier 4 = %s, want 400ms", got)
	}

	backoff.onNoProgressViewChange(4)
	backoff.onNoProgressViewChange(5)
	if got := backoff.state().EffectiveViewChangeResendInterval; got != time.Second {
		t.Fatalf("capped resend interval = %s, want 1s", got)
	}
	if got := backoff.state().EffectiveViewChangeTimeout; got != 1600*time.Millisecond {
		t.Fatalf("view change timeout should continue past resend cap = %s, want 1.6s", got)
	}
}
