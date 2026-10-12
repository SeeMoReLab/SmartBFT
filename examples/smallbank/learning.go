// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	adaptivetimers "github.com/hyperledger-labs/SmartBFT/proto/adaptive_timers"
)

const (
	defaultLearningPollInterval    = 50 * time.Millisecond
	defaultLearningRPCTimeout      = 2 * time.Second
	defaultLearningFeatureDuration = 10 * time.Second
	defaultLearningReplyWait       = 2 * time.Second
	defaultLearningWarmupDuration  = 3 * time.Second
	defaultLearningRewardDuration  = 5 * time.Second

	// An episode whose start is later than this has lost part of its feature
	// window, so the loop skips to the next grid slot still ahead.
	wallClockEpisodeOverrunTolerance = time.Second
)

type learningOptions struct {
	Enabled         bool
	NodeID          uint64
	AgentTarget     string
	InitialTimeout  time.Duration
	FeatureDuration time.Duration
	ReplyWait       time.Duration
	WarmupDuration  time.Duration
	RewardDuration  time.Duration
	// AnchorUnixMS is the apply instant of episode 1, shared by every replica
	// (benchmark start plus the spec warm-up).
	AnchorUnixMS int64
	ApplyTimeout func(time.Duration) error
}

type learningDecision struct {
	timeout time.Duration
}

type pendingLearningReward struct {
	report                 *adaptivetimers.SmartbftReport
	episode                uint32
	timeoutMS              uint32
	throughputTransactions uint64
	throughputDuration     time.Duration
}

type wallClockLearningStage uint8

const (
	wallClockLearningIdle wallClockLearningStage = iota
	wallClockLearningFeature
	wallClockLearningReplyWait
	wallClockLearningWarmup
	wallClockLearningReward
)

type learningManager struct {
	lock sync.Mutex

	enabled bool
	nodeID  uint64
	client  *learningAgentClient

	metrics *learningWindowMetrics

	currentEpisode   uint32
	episodeStartTick uint64

	// episodeAnchor is the apply instant of episode 1 with a monotonic
	// reading; episode e applies at episodeAnchor + period*(e-1).
	episodeAnchor time.Time
	anchorUnixMS  int64

	pollInterval    time.Duration
	rpcTimeout      time.Duration
	featureDuration time.Duration
	replyWait       time.Duration
	warmupDuration  time.Duration
	rewardDuration  time.Duration

	wallClockStage    wallClockLearningStage
	wallClockDeadline time.Time
	wallClockCancel   context.CancelFunc
	lastSequence      uint64

	currentTimeout time.Duration
	lastTimeout    time.Duration

	pollerCancel   context.CancelFunc
	pollerEpisode  uint32
	pollerDecision *learningDecision

	pendingReward *pendingLearningReward

	applyTimeout func(time.Duration) error
}

func learningPrintf(format string, args ...any) {
	prefixedArgs := make([]any, 0, len(args)+1)
	prefixedArgs = append(prefixedArgs, timestampedLogTag("learning"))
	prefixedArgs = append(prefixedArgs, args...)
	fmt.Printf("%s "+format, prefixedArgs...)
}

func newLearningManager(opts learningOptions) (*learningManager, error) {
	if !opts.Enabled {
		return disabledLearningManager(), nil
	}
	if opts.AgentTarget == "" {
		return nil, fmt.Errorf("--learning requires --agent-target")
	}
	if opts.AnchorUnixMS <= 0 {
		return nil, fmt.Errorf("--learning requires --learning-anchor-unix-ms")
	}
	if opts.InitialTimeout <= 0 {
		opts.InitialTimeout = 5 * time.Second
	}
	if opts.FeatureDuration <= 0 {
		opts.FeatureDuration = defaultLearningFeatureDuration
	}
	if opts.ReplyWait <= 0 {
		opts.ReplyWait = defaultLearningReplyWait
	}
	if opts.WarmupDuration <= 0 {
		opts.WarmupDuration = defaultLearningWarmupDuration
	}
	if opts.RewardDuration <= 0 {
		opts.RewardDuration = defaultLearningRewardDuration
	}

	client, err := newLearningAgentClient(opts.AgentTarget, defaultLearningRPCTimeout)
	if err != nil {
		return nil, err
	}

	m := &learningManager{
		enabled:         true,
		nodeID:          opts.NodeID,
		client:          client,
		metrics:         newLearningWindowMetrics(),
		currentEpisode:  1,
		pollInterval:    defaultLearningPollInterval,
		rpcTimeout:      defaultLearningRPCTimeout,
		featureDuration: opts.FeatureDuration,
		replyWait:       opts.ReplyWait,
		warmupDuration:  opts.WarmupDuration,
		rewardDuration:  opts.RewardDuration,
		currentTimeout:  opts.InitialTimeout,
		lastTimeout:     opts.InitialTimeout,
		applyTimeout:    opts.ApplyTimeout,
	}
	learningPrintf("node %d target %s protocol=PBFT initial_timeout_ms=%d window_mode=wall-clock\n",
		opts.NodeID, opts.AgentTarget, opts.InitialTimeout.Milliseconds())
	learningPrintf("wall-clock windows: feature=%s reply_wait=%s warmup=%s reward=%s\n",
		opts.FeatureDuration, opts.ReplyWait, opts.WarmupDuration, opts.RewardDuration)

	// The anchor moves to the monotonic clock once; every deadline derives from it.
	now := time.Now()
	m.episodeAnchor = now.Add(time.UnixMilli(opts.AnchorUnixMS).Sub(now))
	m.anchorUnixMS = opts.AnchorUnixMS
	return m, nil
}

// start launches the anchored episode loop. The caller invokes it once the
// node that applyTimeout reaches is in place.
func (m *learningManager) start() {
	if m == nil || !m.enabled {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.currentEpisode = m.firstWallClockEpisodeAfter(time.Now())
	learningPrintf("episode anchor: anchor_unix_ms=%d period_ms=%d apply_offset_ms=%d first_episode=%d\n",
		m.anchorUnixMS, m.episodePeriod().Milliseconds(), m.applyOffset().Milliseconds(), m.currentEpisode)
	ctx, cancel := context.WithCancel(context.Background())
	m.wallClockCancel = cancel
	go m.runWallClockLearning(ctx, m.currentEpisode)
}

func (m *learningManager) episodePeriod() time.Duration {
	return m.featureDuration + m.replyWait + m.warmupDuration + m.rewardDuration
}

// applyOffset is the time from an episode's start to its apply instant.
func (m *learningManager) applyOffset() time.Duration {
	return m.featureDuration + m.replyWait
}

func (m *learningManager) wallClockEpisodeStart(episode uint32) time.Time {
	return m.episodeAnchor.Add(time.Duration(episode-1)*m.episodePeriod() - m.applyOffset())
}

func (m *learningManager) gridApplyUnixMS(episode uint32) int64 {
	return m.anchorUnixMS + int64(episode-1)*m.episodePeriod().Milliseconds()
}

// firstWallClockEpisodeAfter returns the smallest episode e >= 1 whose start
// is not before now.
func (m *learningManager) firstWallClockEpisodeAfter(now time.Time) uint32 {
	sinceFirstStart := now.Sub(m.wallClockEpisodeStart(1))
	if sinceFirstStart <= 0 {
		return 1
	}
	period := m.episodePeriod()
	return uint32((sinceFirstStart+period-1)/period) + 1
}

func disabledLearningManager() *learningManager {
	return &learningManager{metrics: newLearningWindowMetrics()}
}

func (m *learningManager) close() {
	if m == nil {
		return
	}
	m.lock.Lock()
	if m.wallClockCancel != nil {
		m.wallClockCancel()
		m.wallClockCancel = nil
	}
	m.stopTimeoutPollingLocked()
	client := m.client
	m.lock.Unlock()
	if client != nil {
		client.close()
	}
}

func (m *learningManager) currentTimeoutValue() time.Duration {
	if m == nil {
		return 0
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.currentTimeout
}

func (m *learningManager) recordConsensus(sample learningSample) {
	if m == nil || !m.enabled {
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	m.lastSequence = sample.Sequence
	if sample.DecisionTime.IsZero() {
		sample.DecisionTime = time.Now()
	}

	// Without an anchor (unit tests only) the first delivery starts episode 1.
	if m.wallClockStage == wallClockLearningIdle && m.episodeAnchor.IsZero() {
		startTick := sample.Sequence
		if startTick > 0 {
			startTick--
		}
		m.startWallClockLearningLocked(startTick, sample.DecisionTime)
	}
	if (m.wallClockStage == wallClockLearningFeature || m.wallClockStage == wallClockLearningReward) &&
		(m.wallClockDeadline.IsZero() || !sample.DecisionTime.After(m.wallClockDeadline)) {
		m.metrics.record(sample)
	}
}

func (m *learningManager) recordViewChange() {
	if m == nil || !m.enabled {
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	if m.wallClockStage != wallClockLearningFeature && m.wallClockStage != wallClockLearningReward {
		return
	}
	m.metrics.recordViewChange()
}

func (m *learningManager) startWallClockLearningLocked(startTick uint64, start time.Time) {
	m.episodeAnchor = start.Add(m.applyOffset() - time.Duration(m.currentEpisode-1)*m.episodePeriod())
	m.anchorUnixMS = m.episodeAnchor.UnixMilli()
	m.startWallClockEpisodeLocked(m.currentEpisode, start)
	m.episodeStartTick = startTick

	ctx, cancel := context.WithCancel(context.Background())
	m.wallClockCancel = cancel
	go m.runWallClockLearning(ctx, m.currentEpisode)
}

func (m *learningManager) runWallClockLearning(ctx context.Context, episode uint32) {
	for {
		episodeStart := m.wallClockEpisodeStart(episode)
		if !waitUntil(ctx, episodeStart) {
			return
		}
		next, ok := m.handleWallClockEpisodeStart(episode, episodeStart)
		if !ok {
			return
		}
		if next != episode {
			episode = next
			continue
		}

		featureEnd := episodeStart.Add(m.featureDuration)
		if !waitUntil(ctx, featureEnd) || !m.handleWallClockFeatureDeadline(episode, featureEnd) {
			return
		}

		applyAt := featureEnd.Add(m.replyWait)
		if !waitUntil(ctx, applyAt) || !m.handleWallClockApplyDeadline(episode, applyAt) {
			return
		}

		rewardStart := applyAt.Add(m.warmupDuration)
		if !waitUntil(ctx, rewardStart) || !m.handleWallClockRewardStart(episode, rewardStart) {
			return
		}

		rewardEnd := rewardStart.Add(m.rewardDuration)
		if !waitUntil(ctx, rewardEnd) || !m.handleWallClockRewardDeadline(episode, rewardEnd) {
			return
		}

		episode++
	}
}

func waitUntil(ctx context.Context, deadline time.Time) bool {
	delay := time.Until(deadline)
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// handleWallClockEpisodeStart starts the episode unless the previous reward
// deadline already did. An episode whose start is past by more than the
// tolerance is skipped; the returned episode is then the next one ahead.
func (m *learningManager) handleWallClockEpisodeStart(episode uint32, start time.Time) (uint32, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.currentEpisode != episode ||
		(m.wallClockStage != wallClockLearningIdle && m.wallClockStage != wallClockLearningFeature) {
		return 0, false
	}

	now := time.Now()
	if late := now.Sub(start); late > wallClockEpisodeOverrunTolerance {
		next := m.firstWallClockEpisodeAfter(now)
		learningPrintf("WARNING: episode overrun: episode=%d started late by %s; skipping to episode=%d\n",
			episode, late, next)
		m.currentEpisode = next
		m.wallClockStage = wallClockLearningIdle
		m.wallClockDeadline = time.Time{}
		return next, true
	}
	if m.wallClockStage == wallClockLearningIdle {
		m.startWallClockEpisodeLocked(episode, start)
	}
	return episode, true
}

func (m *learningManager) handleWallClockFeatureDeadline(episode uint32, deadline time.Time) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.currentEpisode != episode ||
		m.wallClockStage != wallClockLearningFeature {
		return false
	}

	reportSeq := m.lastSequence
	m.sendWallClockStateReportLocked(episode, m.episodeStartTick, reportSeq, deadline)
	m.wallClockStage = wallClockLearningReplyWait
	m.wallClockDeadline = deadline.Add(m.replyWait)
	m.startTimeoutPollingLocked(episode)
	return true
}

func (m *learningManager) handleWallClockApplyDeadline(episode uint32, deadline time.Time) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.currentEpisode != episode ||
		m.wallClockStage != wallClockLearningReplyWait {
		return false
	}

	decision := m.pollerDecision
	if m.pollerEpisode != episode {
		decision = nil
	}
	m.stopTimeoutPollingLocked()
	m.pollerEpisode = 0
	m.pollerDecision = nil

	applied := false
	if decision != nil && decision.timeout > 0 {
		if err := m.applyRecommendedTimeoutLocked(decision.timeout); err != nil {
			learningPrintf("failed to apply wall-clock recommendation: episode=%d timeout_ms=%d err=%v\n",
				episode, decision.timeout.Milliseconds(), err)
		} else {
			applied = true
		}
	}
	m.lastTimeout = m.currentTimeout
	m.wallClockStage = wallClockLearningWarmup
	m.wallClockDeadline = deadline.Add(m.warmupDuration)
	gridApplyUnixMS := m.gridApplyUnixMS(episode)
	lagMS := time.Since(deadline).Milliseconds()
	if applied {
		learningPrintf("applied wall-clock recommendation: episode=%d timeout_ms=%d grid_apply_unix_ms=%d lag_ms=%d\n",
			episode, m.currentTimeout.Milliseconds(), gridApplyUnixMS, lagMS)
	} else {
		learningPrintf("wall-clock reply deadline reached without recommendation update: episode=%d timeout_ms=%d grid_apply_unix_ms=%d lag_ms=%d\n",
			episode, m.currentTimeout.Milliseconds(), gridApplyUnixMS, lagMS)
	}
	return true
}

func (m *learningManager) handleWallClockRewardStart(episode uint32, start time.Time) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.currentEpisode != episode ||
		m.wallClockStage != wallClockLearningWarmup {
		return false
	}

	m.metrics.resetWithThroughputStart(start)
	m.metrics.timeout = m.lastTimeout
	m.wallClockStage = wallClockLearningReward
	m.wallClockDeadline = start.Add(m.rewardDuration)
	learningPrintf("started wall-clock reward measurement: episode=%d duration=%s timeout_ms=%d\n",
		episode, m.rewardDuration, m.lastTimeout.Milliseconds())
	return true
}

func (m *learningManager) handleWallClockRewardDeadline(episode uint32, end time.Time) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.currentEpisode != episode ||
		m.wallClockStage != wallClockLearningReward {
		return false
	}

	m.captureRewardUntilLocked(episode, end)
	rewardConsensus := m.metrics.totalConsensus
	rewardTransactions := m.metrics.totalTransactions
	learningPrintf("captured wall-clock reward: episode=%d duration=%s total_consensus=%d total_transactions=%d timeout_ms=%d\n",
		episode, m.rewardDuration, rewardConsensus, rewardTransactions, m.lastTimeout.Milliseconds())
	m.startWallClockEpisodeLocked(episode+1, end)
	return true
}

func (m *learningManager) startWallClockEpisodeLocked(episode uint32, start time.Time) {
	m.currentEpisode = episode
	m.episodeStartTick = m.lastSequence
	m.pollerDecision = nil
	m.pollerEpisode = 0
	m.metrics.resetWithThroughputStart(start)
	m.metrics.timeout = m.currentTimeout
	m.wallClockStage = wallClockLearningFeature
	m.wallClockDeadline = start.Add(m.featureDuration)
}

func (m *learningManager) applyRecommendedTimeoutLocked(timeout time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("non-positive timeout: %s", timeout)
	}
	if m.applyTimeout != nil {
		if err := m.applyTimeout(timeout); err != nil {
			return err
		}
	}
	m.currentTimeout = timeout
	return nil
}

func (m *learningManager) sendWallClockStateReportLocked(episode uint32, startTick uint64, reportSeq uint64, end time.Time) {
	report := m.metrics.buildReportUntil(end)
	reportThroughput := m.metrics.calculateThroughputUntil(end)
	pendingReward := m.pendingReward

	local := &adaptivetimers.ReportLocal{
		NodeId:               uint32(m.nodeID),
		Episode:              episode,
		Protocol:             adaptivetimers.Protocol_PROTOCOL_SMARTBFT,
		StartTick:            saturatingUint32(startTick),
		ReportSeq:            saturatingUint32(reportSeq),
		WindowConsensusCount: 0,
		State:                &adaptivetimers.ReportLocal_SmartbftState{SmartbftState: report},
	}
	if pendingReward != nil {
		local.Reward = &adaptivetimers.Reward{
			Value: &adaptivetimers.Reward_Smartbft{
				Smartbft: &adaptivetimers.SmartbftReward{
					Episode: pendingReward.episode,
					Report:  pendingReward.report,
					TimeoutUsed: &adaptivetimers.SmartbftTimeout{
						ElectionTimeoutMilliseconds: pendingReward.timeoutMS,
					},
				},
			},
		}
	}

	go m.sendWallClockStateReport(local, reportThroughput, pendingReward)
}

func (m *learningManager) sendWallClockStateReport(local *adaptivetimers.ReportLocal, throughput throughputCalculation, pendingReward *pendingLearningReward) {
	ctx, cancel := context.WithTimeout(context.Background(), m.rpcTimeout)
	defer cancel()
	if err := m.client.sendReport(ctx, local); err != nil {
		learningPrintf("SendReport failed: episode=%d target_node=%d window_mode=wall-clock err=%v\n",
			local.Episode, m.nodeID, err)
		return
	}

	report := local.GetSmartbftState()
	learningPrintf("sent report: node=%d episode=%d start_tick=%d report_seq=%d window_mode=wall-clock total_consensus=%d total_transactions=%d throughput_duration_s=%.6f throughput_tps=%.6f\n",
		m.nodeID, local.Episode, local.StartTick, local.ReportSeq, report.TotalConsensusInstances,
		throughput.totalTransactions, throughput.duration.Seconds(), report.ThroughputTps)
	if pendingReward == nil {
		return
	}

	learningPrintf("sent reward: node=%d episode=%d total_transactions=%d throughput_duration_s=%.6f throughput_tps=%.6f\n",
		m.nodeID, pendingReward.episode, pendingReward.throughputTransactions,
		pendingReward.throughputDuration.Seconds(), pendingReward.report.ThroughputTps)
	m.lock.Lock()
	if m.pendingReward == pendingReward {
		m.pendingReward = nil
	}
	m.lock.Unlock()
}

func (m *learningManager) startTimeoutPollingLocked(episode uint32) {
	if m.pollerEpisode == episode && m.pollerCancel != nil {
		return
	}
	m.stopTimeoutPollingLocked()
	ctx, cancel := context.WithCancel(context.Background())
	m.pollerCancel = cancel
	m.pollerEpisode = episode
	m.pollerDecision = nil

	go m.pollForTimeout(ctx, episode)
}

func (m *learningManager) stopTimeoutPollingLocked() {
	if m.pollerCancel != nil {
		m.pollerCancel()
		m.pollerCancel = nil
	}
}

func (m *learningManager) pollForTimeout(ctx context.Context, episode uint32) {
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rpcCtx, cancel := context.WithTimeout(ctx, m.rpcTimeout)
			status, err := m.client.getTimeout(rpcCtx, episode)
			cancel()
			if err != nil || status == nil {
				continue
			}
			if status.Status != adaptivetimers.TimeoutStatus_READY || status.Timeout == nil || status.Timeout.GetSmartbft() == nil {
				continue
			}
			// A wall-clock decision echoes window_consensus_count = 0.
			startTick := uint64(status.StartTick)
			reportSeq := uint64(status.ReportSeq)
			if reportSeq < startTick || status.WindowConsensusCount != 0 {
				continue
			}
			timeout := time.Duration(status.Timeout.GetSmartbft().ElectionTimeoutMilliseconds) * time.Millisecond
			if timeout <= 0 {
				continue
			}

			m.lock.Lock()
			if m.pollerEpisode == episode && m.pollerDecision == nil {
				m.pollerDecision = &learningDecision{timeout: timeout}
				learningPrintf("timeout READY: episode=%d start_tick=%d report_seq=%d window_consensus_count=%d timeout_ms=%d\n",
					episode, startTick, reportSeq, status.WindowConsensusCount, timeout.Milliseconds())
			}
			m.lock.Unlock()
			return
		}
	}
}

func (m *learningManager) captureRewardUntilLocked(episode uint32, end time.Time) {
	report := m.metrics.buildReportUntil(end)
	throughput := m.metrics.calculateThroughputUntil(end)
	m.pendingReward = &pendingLearningReward{
		report:                 report,
		episode:                episode,
		timeoutMS:              uint32(max(int64(0), m.lastTimeout.Milliseconds())),
		throughputTransactions: throughput.totalTransactions,
		throughputDuration:     throughput.duration,
	}
}
