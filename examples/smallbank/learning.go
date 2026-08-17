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
	defaultLearningReportTickInterval = 10
	defaultLearningReportTrigger      = 5 * time.Second
	defaultLearningMaxReportLength    = 500
	defaultLearningPollInterval       = 50 * time.Millisecond
	defaultLearningRPCTimeout         = 2 * time.Second
	defaultLearningFeatureDuration    = 10 * time.Second
	defaultLearningReplyWait          = 2 * time.Second
	defaultLearningWarmupDuration     = 3 * time.Second
	defaultLearningRewardDuration     = 5 * time.Second
)

type learningWindowMode string

const (
	learningWindowModeConsensus learningWindowMode = "consensus"
	learningWindowModeWallClock learningWindowMode = "wall-clock"
)

func parseLearningWindowMode(raw string) (learningWindowMode, error) {
	mode := learningWindowMode(raw)
	switch mode {
	case learningWindowModeConsensus, learningWindowModeWallClock:
		return mode, nil
	default:
		return "", fmt.Errorf("unknown learning window mode %q; expected consensus or wall-clock", raw)
	}
}

type learningOptions struct {
	Enabled            bool
	NodeID             uint64
	AgentTarget        string
	InitialTimeout     time.Duration
	WindowMode         learningWindowMode
	ReportTickInterval uint64
	ReportTrigger      time.Duration
	MaxReportLength    uint64
	PollInterval       time.Duration
	RPCTimeout         time.Duration
	FeatureDuration    time.Duration
	ReplyWait          time.Duration
	WarmupDuration     time.Duration
	RewardDuration     time.Duration
	ApplyTimeout       func(time.Duration) error
}

type learningDecision struct {
	timeout        time.Duration
	startTick      uint64
	reportSeq      uint64
	reportEndCount uint64
	reportLength   uint64
}

type learningEpisodeWindow struct {
	startTick        uint64
	reportSeq        uint64
	reportLen        uint64
	reportEndCount   uint64
	applyCount       uint64
	rewardStartCount uint64
	rewardEndCount   uint64
}

type learningReportWindow struct {
	endCount uint64
	length   uint64
}

type pendingLearningReward struct {
	report                 *adaptivetimers.PbftReport
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

	currentEpisode       uint32
	episodeStartTick     uint64
	episodeStartCount    uint64
	deliveredCount       uint64
	episodeStartWallTime time.Time

	reportTickInterval uint64
	reportTrigger      time.Duration
	maxReportLength    uint64
	pollInterval       time.Duration
	rpcTimeout         time.Duration
	windowMode         learningWindowMode
	featureDuration    time.Duration
	replyWait          time.Duration
	warmupDuration     time.Duration
	rewardDuration     time.Duration

	wallClockStage    wallClockLearningStage
	wallClockDeadline time.Time
	wallClockCancel   context.CancelFunc
	lastSequence      uint64

	currentTimeout time.Duration
	lastTimeout    time.Duration

	reportSentForEpisode       bool
	reachedReportCapForEpisode bool
	capApplyDeadlineCount      uint64
	capRewardStartCount        uint64
	capRewardDeadlineCount     uint64
	selectedWindow             *learningEpisodeWindow
	selectedTimeout            time.Duration
	applyHandledForEpisode     bool
	rewardMetricsStarted       bool
	rewardCapturedForEpisode   bool
	waitingForRecommendation   bool

	pollerCancel   context.CancelFunc
	pollerEpisode  uint32
	pollerDecision *learningDecision
	reportWindows  map[uint64]learningReportWindow

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
	if opts.InitialTimeout <= 0 {
		opts.InitialTimeout = 5 * time.Second
	}
	if opts.ReportTickInterval == 0 {
		opts.ReportTickInterval = defaultLearningReportTickInterval
	}
	if opts.ReportTrigger <= 0 {
		opts.ReportTrigger = defaultLearningReportTrigger
	}
	if opts.MaxReportLength == 0 {
		opts.MaxReportLength = defaultLearningMaxReportLength
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = defaultLearningPollInterval
	}
	if opts.RPCTimeout <= 0 {
		opts.RPCTimeout = defaultLearningRPCTimeout
	}
	if opts.WindowMode == "" {
		opts.WindowMode = learningWindowModeConsensus
	}
	if _, err := parseLearningWindowMode(string(opts.WindowMode)); err != nil {
		return nil, err
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

	client, err := newLearningAgentClient(opts.AgentTarget, opts.RPCTimeout)
	if err != nil {
		return nil, err
	}

	m := &learningManager{
		enabled:            true,
		nodeID:             opts.NodeID,
		client:             client,
		metrics:            newLearningWindowMetrics(),
		currentEpisode:     1,
		reportTickInterval: opts.ReportTickInterval,
		reportTrigger:      opts.ReportTrigger,
		maxReportLength:    opts.MaxReportLength,
		pollInterval:       opts.PollInterval,
		rpcTimeout:         opts.RPCTimeout,
		windowMode:         opts.WindowMode,
		featureDuration:    opts.FeatureDuration,
		replyWait:          opts.ReplyWait,
		warmupDuration:     opts.WarmupDuration,
		rewardDuration:     opts.RewardDuration,
		currentTimeout:     opts.InitialTimeout,
		lastTimeout:        opts.InitialTimeout,
		applyTimeout:       opts.ApplyTimeout,
		reportWindows:      make(map[uint64]learningReportWindow),
	}
	learningPrintf("node %d target %s protocol=PBFT initial_timeout_ms=%d window_mode=%s\n",
		opts.NodeID, opts.AgentTarget, opts.InitialTimeout.Milliseconds(), opts.WindowMode)
	if opts.WindowMode == learningWindowModeWallClock {
		learningPrintf("wall-clock windows: feature=%s reply_wait=%s warmup=%s reward=%s\n",
			opts.FeatureDuration, opts.ReplyWait, opts.WarmupDuration, opts.RewardDuration)
	}
	return m, nil
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

	m.deliveredCount++
	deliveredCount := m.deliveredCount
	m.lastSequence = sample.Sequence
	if sample.DecisionTime.IsZero() {
		sample.DecisionTime = time.Now()
	}

	if m.windowMode == learningWindowModeWallClock {
		if m.wallClockStage == wallClockLearningIdle {
			startTick := sample.Sequence
			if startTick > 0 {
				startTick--
			}
			m.startWallClockLearningLocked(startTick, deliveredCount-1, sample.DecisionTime)
		}
		if (m.wallClockStage == wallClockLearningFeature || m.wallClockStage == wallClockLearningReward) &&
			(m.wallClockDeadline.IsZero() || !sample.DecisionTime.After(m.wallClockDeadline)) {
			m.metrics.record(sample)
		}
		return
	}

	if m.episodeStartWallTime.IsZero() {
		m.episodeStartWallTime = time.Now()
		m.episodeStartCount = deliveredCount
		m.episodeStartTick = sample.Sequence
		m.metrics.resetWithThroughputStart(sample.DecisionTime)
	} else {
		m.metrics.record(sample)
	}

	m.maybeConsumeRecommendationLocked(sample.Sequence, deliveredCount)
	if m.isReportTick(deliveredCount) {
		m.maybeSendReportTickLocked(sample.Sequence, deliveredCount)
	}
	m.maybeHandleApplyDeadlineLocked(sample.Sequence, deliveredCount)
	m.maybeHandleRewardStartLocked(sample, deliveredCount)
	m.maybeHandleRewardDeadlineLocked(sample, deliveredCount)
}

func (m *learningManager) isReportTick(deliveredCount uint64) bool {
	if m.reportTickInterval == 0 || deliveredCount <= m.episodeStartCount {
		return false
	}
	return (deliveredCount-m.episodeStartCount)%m.reportTickInterval == 0
}

func (m *learningManager) recordViewChange() {
	if m == nil || !m.enabled {
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	if m.windowMode == learningWindowModeWallClock &&
		m.wallClockStage != wallClockLearningFeature &&
		m.wallClockStage != wallClockLearningReward {
		return
	}
	m.metrics.recordViewChange()
}

func (m *learningManager) recordNoProgressViewChange() {
	if m == nil || !m.enabled {
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	if m.windowMode == learningWindowModeWallClock &&
		m.wallClockStage != wallClockLearningFeature &&
		m.wallClockStage != wallClockLearningReward {
		return
	}
	m.metrics.recordNoProgressViewChange()
}

func (m *learningManager) startWallClockLearningLocked(startTick uint64, startCount uint64, start time.Time) {
	m.episodeStartTick = startTick
	m.episodeStartCount = startCount
	m.episodeStartWallTime = start
	m.wallClockStage = wallClockLearningFeature
	m.wallClockDeadline = start.Add(m.featureDuration)
	m.metrics.resetWithThroughputStart(start)
	m.metrics.timeout = m.currentTimeout

	ctx, cancel := context.WithCancel(context.Background())
	m.wallClockCancel = cancel
	go m.runWallClockLearning(ctx, m.currentEpisode, start)
}

func (m *learningManager) runWallClockLearning(ctx context.Context, episode uint32, episodeStart time.Time) {
	for {
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
		episodeStart = rewardEnd
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

func (m *learningManager) handleWallClockFeatureDeadline(episode uint32, deadline time.Time) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.windowMode != learningWindowModeWallClock ||
		m.currentEpisode != episode ||
		m.wallClockStage != wallClockLearningFeature {
		return false
	}

	reportLength := m.metrics.totalConsensus
	reportSeq := m.lastSequence
	m.reportWindows = map[uint64]learningReportWindow{
		0: {
			endCount: m.deliveredCount,
			length:   reportLength,
		},
	}
	m.sendWallClockStateReportLocked(episode, m.episodeStartTick, reportSeq, deadline)
	m.reportSentForEpisode = true
	m.waitingForRecommendation = true
	m.wallClockStage = wallClockLearningReplyWait
	m.wallClockDeadline = deadline.Add(m.replyWait)
	m.startTimeoutPollingLocked(episode)
	return true
}

func (m *learningManager) handleWallClockApplyDeadline(episode uint32, deadline time.Time) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.windowMode != learningWindowModeWallClock ||
		m.currentEpisode != episode ||
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
	m.waitingForRecommendation = false
	m.applyHandledForEpisode = true

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
	if applied {
		learningPrintf("applied wall-clock recommendation: episode=%d timeout_ms=%d\n",
			episode, m.currentTimeout.Milliseconds())
	} else {
		learningPrintf("wall-clock reply deadline reached without recommendation update: episode=%d timeout_ms=%d\n",
			episode, m.currentTimeout.Milliseconds())
	}
	return true
}

func (m *learningManager) handleWallClockRewardStart(episode uint32, start time.Time) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.windowMode != learningWindowModeWallClock ||
		m.currentEpisode != episode ||
		m.wallClockStage != wallClockLearningWarmup {
		return false
	}

	m.metrics.resetWithThroughputStart(start)
	m.metrics.timeout = m.lastTimeout
	m.rewardMetricsStarted = true
	m.wallClockStage = wallClockLearningReward
	m.wallClockDeadline = start.Add(m.rewardDuration)
	learningPrintf("started wall-clock reward measurement: episode=%d duration=%s timeout_ms=%d\n",
		episode, m.rewardDuration, m.lastTimeout.Milliseconds())
	return true
}

func (m *learningManager) handleWallClockRewardDeadline(episode uint32, end time.Time) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.windowMode != learningWindowModeWallClock ||
		m.currentEpisode != episode ||
		m.wallClockStage != wallClockLearningReward {
		return false
	}

	m.captureRewardUntilLocked(episode, end, true)
	m.rewardCapturedForEpisode = true
	rewardConsensus := m.metrics.totalConsensus
	rewardTransactions := m.metrics.totalTransactions
	learningPrintf("captured wall-clock reward: episode=%d duration=%s total_consensus=%d total_transactions=%d timeout_ms=%d\n",
		episode, m.rewardDuration, rewardConsensus, rewardTransactions, m.lastTimeout.Milliseconds())
	m.startNextWallClockEpisodeLocked(end)
	return true
}

func (m *learningManager) startNextWallClockEpisodeLocked(start time.Time) {
	m.currentEpisode++
	m.episodeStartTick = m.lastSequence
	m.episodeStartCount = m.deliveredCount
	m.episodeStartWallTime = start
	m.reportSentForEpisode = false
	m.selectedWindow = nil
	m.selectedTimeout = 0
	m.applyHandledForEpisode = false
	m.rewardMetricsStarted = false
	m.rewardCapturedForEpisode = false
	m.waitingForRecommendation = false
	m.pollerDecision = nil
	m.pollerEpisode = 0
	m.reportWindows = make(map[uint64]learningReportWindow)
	m.metrics.resetWithThroughputStart(start)
	m.metrics.timeout = m.currentTimeout
	m.wallClockStage = wallClockLearningFeature
	m.wallClockDeadline = start.Add(m.featureDuration)
}

func (m *learningManager) maybeSendReportTickLocked(sequence uint64, deliveredCount uint64) {
	if m.selectedWindow != nil || m.reachedReportCapForEpisode {
		return
	}
	reportLength := deliveredCount - m.episodeStartCount
	if reportLength == 0 {
		return
	}
	elapsed := time.Since(m.episodeStartWallTime)
	shouldSendInitial := !m.reportSentForEpisode && (elapsed >= m.reportTrigger || reportLength >= m.maxReportLength)
	shouldSendFollowup := m.reportSentForEpisode && m.waitingForRecommendation
	if !shouldSendInitial && !shouldSendFollowup {
		return
	}

	reportSeq := sequence
	if m.reportWindows == nil {
		m.reportWindows = make(map[uint64]learningReportWindow)
	}
	m.reportWindows[reportLength] = learningReportWindow{
		endCount: deliveredCount,
		length:   reportLength,
	}
	m.sendStateReportLocked(m.currentEpisode, m.episodeStartTick, reportSeq, reportLength)
	m.reportSentForEpisode = true
	m.waitingForRecommendation = true
	m.startTimeoutPollingLocked(m.currentEpisode)

	if reportLength >= m.maxReportLength {
		m.reachedReportCapForEpisode = true
		window := newLearningEpisodeWindow(m.episodeStartTick, reportSeq, deliveredCount, reportLength)
		m.capApplyDeadlineCount = window.applyCount
		m.capRewardStartCount = window.rewardStartCount
		m.capRewardDeadlineCount = window.rewardEndCount
	}
}

func (m *learningManager) maybeConsumeRecommendationLocked(sequence uint64, deliveredCount uint64) {
	if m.selectedWindow != nil || m.pollerDecision == nil || m.pollerEpisode != m.currentEpisode {
		return
	}

	decision := m.pollerDecision
	window := newLearningEpisodeWindow(decision.startTick, decision.reportSeq, decision.reportEndCount, decision.reportLength)
	m.selectedWindow = window
	m.selectedTimeout = decision.timeout
	m.waitingForRecommendation = false
	m.stopTimeoutPollingLocked()
	learningPrintf("received recommendation: episode=%d report_seq=%d apply_count=%d reward_start_count=%d reward_end_count=%d timeout_ms=%d current_seq=%d delivered_count=%d\n",
		m.currentEpisode, window.reportSeq, window.applyCount, window.rewardStartCount, window.rewardEndCount, decision.timeout.Milliseconds(), sequence, deliveredCount)

	if deliveredCount > window.applyCount {
		m.applyHandledForEpisode = true
		m.lastTimeout = m.currentTimeout
		learningPrintf("ignored late recommendation: episode=%d report_seq=%d apply_count=%d current_seq=%d delivered_count=%d\n",
			m.currentEpisode, window.reportSeq, window.applyCount, sequence, deliveredCount)
	}
}

func (m *learningManager) maybeHandleApplyDeadlineLocked(sequence uint64, deliveredCount uint64) {
	if m.applyHandledForEpisode {
		return
	}
	if m.selectedWindow != nil {
		if deliveredCount < m.selectedWindow.applyCount {
			return
		}
		applied := false
		var applyErr error
		recommendedTimeout := m.selectedTimeout
		if recommendedTimeout > 0 {
			if err := m.applyRecommendedTimeoutLocked(recommendedTimeout); err != nil {
				applyErr = err
				learningPrintf("failed to apply recommendation: episode=%d report_seq=%d apply_count=%d current_seq=%d delivered_count=%d timeout_ms=%d err=%v\n",
					m.currentEpisode, m.selectedWindow.reportSeq, m.selectedWindow.applyCount, sequence, deliveredCount, recommendedTimeout.Milliseconds(), err)
			} else {
				applied = true
			}
		}
		m.applyHandledForEpisode = true
		m.lastTimeout = m.currentTimeout
		if applied {
			m.rebaseSelectedWindowAfterApplyLocked(deliveredCount)
			m.metrics.resetWithThroughputStart(time.Now())
			learningPrintf("applied recommendation: episode=%d report_seq=%d apply_count=%d reward_start_count=%d reward_end_count=%d current_seq=%d delivered_count=%d timeout_ms=%d\n",
				m.currentEpisode, m.selectedWindow.reportSeq, m.selectedWindow.applyCount, m.selectedWindow.rewardStartCount, m.selectedWindow.rewardEndCount, sequence, deliveredCount, m.currentTimeout.Milliseconds())
		} else if applyErr != nil {
			learningPrintf("apply deadline reached with recommendation apply failure: episode=%d report_seq=%d apply_count=%d current_seq=%d delivered_count=%d recommended_timeout_ms=%d current_timeout_ms=%d err=%v\n",
				m.currentEpisode, m.selectedWindow.reportSeq, m.selectedWindow.applyCount, sequence, deliveredCount, recommendedTimeout.Milliseconds(), m.currentTimeout.Milliseconds(), applyErr)
		} else {
			learningPrintf("apply deadline reached without recommendation update: episode=%d report_seq=%d apply_count=%d current_seq=%d delivered_count=%d timeout_ms=%d\n",
				m.currentEpisode, m.selectedWindow.reportSeq, m.selectedWindow.applyCount, sequence, deliveredCount, m.currentTimeout.Milliseconds())
		}
		return
	}
	if m.reachedReportCapForEpisode && m.capApplyDeadlineCount > 0 && deliveredCount >= m.capApplyDeadlineCount {
		m.waitingForRecommendation = false
		m.applyHandledForEpisode = true
		m.lastTimeout = m.currentTimeout
		m.stopTimeoutPollingLocked()
		learningPrintf("recommendation unavailable at cap apply deadline: episode=%d cap_apply_count=%d current_seq=%d delivered_count=%d timeout_ms=%d\n",
			m.currentEpisode, m.capApplyDeadlineCount, sequence, deliveredCount, m.currentTimeout.Milliseconds())
	}
}

func (m *learningManager) rebaseSelectedWindowAfterApplyLocked(deliveredCount uint64) {
	if m.selectedWindow == nil || deliveredCount <= m.selectedWindow.applyCount {
		return
	}
	applyToRewardStart := m.selectedWindow.rewardStartCount - m.selectedWindow.applyCount
	m.selectedWindow.applyCount = deliveredCount
	m.selectedWindow.rewardStartCount = deliveredCount + applyToRewardStart
	m.selectedWindow.rewardEndCount = m.selectedWindow.rewardStartCount + m.selectedWindow.reportLen
}

func (m *learningManager) maybeHandleRewardStartLocked(sample learningSample, deliveredCount uint64) {
	if m.rewardMetricsStarted || m.rewardCapturedForEpisode {
		return
	}
	var rewardStartCount uint64
	source := "none"
	if m.selectedWindow != nil {
		rewardStartCount = m.selectedWindow.rewardStartCount
		source = "recommended_window"
	} else if m.reachedReportCapForEpisode && m.capRewardStartCount > 0 {
		rewardStartCount = m.capRewardStartCount
		source = "cap_window"
	}
	if rewardStartCount == 0 || deliveredCount < rewardStartCount {
		return
	}

	m.metrics.resetWithThroughputStart(sample.DecisionTime)
	m.rewardMetricsStarted = true
	learningPrintf("started reward measurement: episode=%d reward_start_count=%d current_seq=%d delivered_count=%d source=%s timeout_ms=%d\n",
		m.currentEpisode, rewardStartCount, sample.Sequence, deliveredCount, source, m.lastTimeout.Milliseconds())
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

func (m *learningManager) maybeHandleRewardDeadlineLocked(sample learningSample, deliveredCount uint64) {
	if m.rewardCapturedForEpisode {
		return
	}
	var rewardDeadlineCount uint64
	source := "none"
	if m.selectedWindow != nil {
		rewardDeadlineCount = m.selectedWindow.rewardEndCount
		source = "recommended_window"
	} else if m.reachedReportCapForEpisode && m.capRewardDeadlineCount > 0 {
		rewardDeadlineCount = m.capRewardDeadlineCount
		source = "cap_window"
	}
	if rewardDeadlineCount == 0 || deliveredCount < rewardDeadlineCount {
		return
	}
	if !m.rewardMetricsStarted {
		return
	}

	m.captureRewardLocked(m.currentEpisode)
	m.rewardCapturedForEpisode = true
	m.stopTimeoutPollingLocked()
	learningPrintf("captured reward: episode=%d reward_end_count=%d current_seq=%d delivered_count=%d source=%s timeout_ms=%d\n",
		m.currentEpisode, rewardDeadlineCount, sample.Sequence, deliveredCount, source, m.lastTimeout.Milliseconds())
	m.startNextEpisodeLocked(sample.Sequence, deliveredCount, sample.DecisionTime)
}

func (m *learningManager) sendStateReportLocked(episode uint32, startTick uint64, reportSeq uint64, reportLength uint64) {
	report := m.metrics.buildReport()
	if report == nil {
		learningPrintf("episode %d skipped report: metrics unavailable\n", episode)
		return
	}
	reportThroughput := m.metrics.calculateThroughput()

	local := &adaptivetimers.ReportLocal{
		NodeId:               uint32(m.nodeID),
		Episode:              episode,
		Protocol:             adaptivetimers.Protocol_PROTOCOL_PBFT,
		StartTick:            saturatingUint32(startTick),
		ReportSeq:            saturatingUint32(reportSeq),
		WindowConsensusCount: saturatingUint32(reportLength),
		State:                &adaptivetimers.ReportLocal_PbftState{PbftState: report},
	}
	if m.pendingReward != nil {
		local.Reward = &adaptivetimers.Reward{
			Value: &adaptivetimers.Reward_Pbft{
				Pbft: &adaptivetimers.PbftReward{
					Episode: m.pendingReward.episode,
					Report:  m.pendingReward.report,
					TimeoutUsed: &adaptivetimers.PbftTimeout{
						ElectionTimeoutMilliseconds: m.pendingReward.timeoutMS,
					},
				},
			},
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), m.rpcTimeout)
	defer cancel()
	if err := m.client.sendReport(ctx, local); err != nil {
		learningPrintf("SendReport failed: episode=%d target_node=%d err=%v\n", episode, m.nodeID, err)
		return
	}
	learningPrintf("sent report: node=%d episode=%d start_tick=%d report_seq=%d window_consensus_count=%d total_transactions=%d throughput_duration_s=%.6f throughput_tps=%.6f\n",
		m.nodeID, episode, startTick, reportSeq, reportLength, reportThroughput.totalTransactions, reportThroughput.duration.Seconds(), report.ThroughputTps)
	if m.pendingReward != nil {
		learningPrintf("sent reward: node=%d episode=%d total_transactions=%d throughput_duration_s=%.6f throughput_tps=%.6f\n",
			m.nodeID, m.pendingReward.episode, m.pendingReward.throughputTransactions, m.pendingReward.throughputDuration.Seconds(), m.pendingReward.report.ThroughputTps)
		m.pendingReward = nil
	}
}

func (m *learningManager) sendWallClockStateReportLocked(episode uint32, startTick uint64, reportSeq uint64, end time.Time) {
	report := m.metrics.buildReportUntil(end, true)
	reportThroughput := m.metrics.calculateThroughputUntil(end)
	pendingReward := m.pendingReward

	local := &adaptivetimers.ReportLocal{
		NodeId:               uint32(m.nodeID),
		Episode:              episode,
		Protocol:             adaptivetimers.Protocol_PROTOCOL_PBFT,
		StartTick:            saturatingUint32(startTick),
		ReportSeq:            saturatingUint32(reportSeq),
		WindowConsensusCount: 0,
		State:                &adaptivetimers.ReportLocal_PbftState{PbftState: report},
	}
	if pendingReward != nil {
		local.Reward = &adaptivetimers.Reward{
			Value: &adaptivetimers.Reward_Pbft{
				Pbft: &adaptivetimers.PbftReward{
					Episode: pendingReward.episode,
					Report:  pendingReward.report,
					TimeoutUsed: &adaptivetimers.PbftTimeout{
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

	report := local.GetPbftState()
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
	reportedUnknownWindow := false

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
			if status.Status != adaptivetimers.TimeoutStatus_READY || status.Timeout == nil || status.Timeout.GetPbft() == nil {
				continue
			}
			startTick := uint64(status.StartTick)
			reportSeq := uint64(status.ReportSeq)
			windowConsensusCount := uint64(status.WindowConsensusCount)
			if m.windowMode == learningWindowModeWallClock {
				if reportSeq < startTick || windowConsensusCount != 0 {
					continue
				}
			} else {
				if reportSeq <= startTick || windowConsensusCount == 0 {
					continue
				}
			}
			timeout := time.Duration(status.Timeout.GetPbft().ElectionTimeoutMilliseconds) * time.Millisecond
			if timeout <= 0 {
				continue
			}

			m.lock.Lock()
			reportWindow, found := m.reportWindows[windowConsensusCount]
			if m.pollerEpisode == episode && m.pollerDecision == nil && found {
				m.pollerDecision = &learningDecision{
					timeout:        timeout,
					startTick:      startTick,
					reportSeq:      reportSeq,
					reportEndCount: reportWindow.endCount,
					reportLength:   reportWindow.length,
				}
				learningPrintf("timeout READY: episode=%d start_tick=%d report_seq=%d window_consensus_count=%d report_end_count=%d timeout_ms=%d\n",
					episode, startTick, reportSeq, windowConsensusCount, reportWindow.endCount, timeout.Milliseconds())
			} else if m.pollerEpisode == episode && m.pollerDecision == nil && !found && !reportedUnknownWindow {
				learningPrintf("ignored timeout for unknown local report window: episode=%d start_tick=%d report_seq=%d window_consensus_count=%d\n",
					episode, startTick, reportSeq, windowConsensusCount)
				reportedUnknownWindow = true
			}
			m.lock.Unlock()
			if found {
				return
			}
		}
	}
}

func (m *learningManager) captureRewardLocked(episode uint32) {
	m.captureRewardUntilLocked(episode, time.Time{}, false)
}

func (m *learningManager) captureRewardUntilLocked(episode uint32, end time.Time, allowEmpty bool) {
	report := m.metrics.buildReportUntil(end, allowEmpty)
	if report == nil {
		return
	}
	throughput := m.metrics.calculateThroughputUntil(end)
	m.pendingReward = &pendingLearningReward{
		report:                 report,
		episode:                episode,
		timeoutMS:              uint32(max(int64(0), m.lastTimeout.Milliseconds())),
		throughputTransactions: throughput.totalTransactions,
		throughputDuration:     throughput.duration,
	}
}

func (m *learningManager) startNextEpisodeLocked(startTick uint64, startCount uint64, startTime time.Time) {
	m.currentEpisode++
	m.episodeStartTick = startTick
	m.episodeStartCount = startCount
	m.episodeStartWallTime = time.Now()
	m.reportSentForEpisode = false
	m.reachedReportCapForEpisode = false
	m.capApplyDeadlineCount = 0
	m.capRewardStartCount = 0
	m.capRewardDeadlineCount = 0
	m.selectedWindow = nil
	m.selectedTimeout = 0
	m.applyHandledForEpisode = false
	m.rewardMetricsStarted = false
	m.rewardCapturedForEpisode = false
	m.waitingForRecommendation = false
	m.pollerDecision = nil
	m.pollerEpisode = 0
	m.reportWindows = make(map[uint64]learningReportWindow)
	m.metrics.resetWithThroughputStart(startTime)
}

func newLearningEpisodeWindow(startTick, reportSeq, reportEndCount, reportLength uint64) *learningEpisodeWindow {
	return &learningEpisodeWindow{
		startTick:        startTick,
		reportSeq:        reportSeq,
		reportLen:        reportLength,
		reportEndCount:   reportEndCount,
		applyCount:       reportEndCount + reportLength/2,
		rewardStartCount: reportEndCount + reportLength,
		rewardEndCount:   reportEndCount + 2*reportLength,
	}
}
