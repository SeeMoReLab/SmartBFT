// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"testing"
	"time"
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

func TestProposalDelayController(t *testing.T) {
	ctrl, err := loadProposalDelayController("testdata/failure_spec.xml", time.Now().Add(-500*time.Millisecond).UnixMilli())
	if err != nil {
		t.Fatalf("load failure spec: %v", err)
	}

	// Failure spec IDs are 0-based, while SmartBFT node IDs are 1-based.
	// With leader node 1 and f=1 for a 4-node cluster, the leader token targets replica 0 / node 1.
	delay := ctrl.delayForProposal(1, 1, []uint64{1, 2, 3, 4})
	if delay != 25*time.Millisecond {
		t.Fatalf("leader delay = %s, want 25ms", delay)
	}

	delay = ctrl.delayForProposal(2, 1, []uint64{1, 2, 3, 4})
	if delay != 0 {
		t.Fatalf("non-leader delay = %s, want 0", delay)
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
		PostDecision: 2 * time.Millisecond,
		Timeout:      15 * time.Millisecond,
	})
	metrics.record(learningSample{
		Sequence:     11,
		View:         1,
		LeaderID:     2,
		BatchSize:    3,
		DecisionTime: start.Add(100 * time.Millisecond),
		Latencies:    []time.Duration{30 * time.Millisecond},
		PostDecision: 4 * time.Millisecond,
		Timeout:      15 * time.Millisecond,
	})

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
	if report.TimeoutViolationRate < 0.66 || report.TimeoutViolationRate > 0.67 {
		t.Fatalf("TimeoutViolationRate = %f, want about 0.67", report.TimeoutViolationRate)
	}
	if report.PhasePostDecisionAvgDelayMs != 3 {
		t.Fatalf("PhasePostDecisionAvgDelayMs = %f, want 3", report.PhasePostDecisionAvgDelayMs)
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
