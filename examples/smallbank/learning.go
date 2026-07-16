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
)

type learningOptions struct {
	Enabled            bool
	NodeID             uint64
	AgentTarget        string
	InitialTimeout     time.Duration
	ReportTickInterval uint64
	ReportTrigger      time.Duration
	MaxReportLength    uint64
	PollInterval       time.Duration
	RPCTimeout         time.Duration
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
		currentTimeout:     opts.InitialTimeout,
		lastTimeout:        opts.InitialTimeout,
		applyTimeout:       opts.ApplyTimeout,
		reportWindows:      make(map[uint64]learningReportWindow),
	}
	fmt.Printf("[learning] node %d target %s protocol=PBFT initial_timeout_ms=%d\n",
		opts.NodeID, opts.AgentTarget, opts.InitialTimeout.Milliseconds())
	return m, nil
}

func disabledLearningManager() *learningManager {
	return &learningManager{metrics: newLearningWindowMetrics()}
}

func (m *learningManager) close() {
	if m == nil {
		return
	}
	m.stopTimeoutPollingLocked()
	if m.client != nil {
		m.client.close()
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

	m.metrics.recordViewChange()
}

func (m *learningManager) recordNoProgressViewChange() {
	if m == nil || !m.enabled {
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	m.metrics.recordNoProgressViewChange()
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
	fmt.Printf("[learning] received recommendation: episode=%d report_seq=%d apply_count=%d reward_start_count=%d reward_end_count=%d timeout_ms=%d current_seq=%d delivered_count=%d\n",
		m.currentEpisode, window.reportSeq, window.applyCount, window.rewardStartCount, window.rewardEndCount, decision.timeout.Milliseconds(), sequence, deliveredCount)

	if deliveredCount > window.applyCount {
		m.applyHandledForEpisode = true
		m.lastTimeout = m.currentTimeout
		fmt.Printf("[learning] ignored late recommendation: episode=%d report_seq=%d apply_count=%d current_seq=%d delivered_count=%d\n",
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
				fmt.Printf("[learning] failed to apply recommendation: episode=%d report_seq=%d apply_count=%d current_seq=%d delivered_count=%d timeout_ms=%d err=%v\n",
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
			fmt.Printf("[learning] applied recommendation: episode=%d report_seq=%d apply_count=%d reward_start_count=%d reward_end_count=%d current_seq=%d delivered_count=%d timeout_ms=%d\n",
				m.currentEpisode, m.selectedWindow.reportSeq, m.selectedWindow.applyCount, m.selectedWindow.rewardStartCount, m.selectedWindow.rewardEndCount, sequence, deliveredCount, m.currentTimeout.Milliseconds())
		} else if applyErr != nil {
			fmt.Printf("[learning] apply deadline reached with recommendation apply failure: episode=%d report_seq=%d apply_count=%d current_seq=%d delivered_count=%d recommended_timeout_ms=%d current_timeout_ms=%d err=%v\n",
				m.currentEpisode, m.selectedWindow.reportSeq, m.selectedWindow.applyCount, sequence, deliveredCount, recommendedTimeout.Milliseconds(), m.currentTimeout.Milliseconds(), applyErr)
		} else {
			fmt.Printf("[learning] apply deadline reached without recommendation update: episode=%d report_seq=%d apply_count=%d current_seq=%d delivered_count=%d timeout_ms=%d\n",
				m.currentEpisode, m.selectedWindow.reportSeq, m.selectedWindow.applyCount, sequence, deliveredCount, m.currentTimeout.Milliseconds())
		}
		return
	}
	if m.reachedReportCapForEpisode && m.capApplyDeadlineCount > 0 && deliveredCount >= m.capApplyDeadlineCount {
		m.waitingForRecommendation = false
		m.applyHandledForEpisode = true
		m.lastTimeout = m.currentTimeout
		m.stopTimeoutPollingLocked()
		fmt.Printf("[learning] recommendation unavailable at cap apply deadline: episode=%d cap_apply_count=%d current_seq=%d delivered_count=%d timeout_ms=%d\n",
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
	fmt.Printf("[learning] started reward measurement: episode=%d reward_start_count=%d current_seq=%d delivered_count=%d source=%s timeout_ms=%d\n",
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
	fmt.Printf("[learning] captured reward: episode=%d reward_end_count=%d current_seq=%d delivered_count=%d source=%s timeout_ms=%d\n",
		m.currentEpisode, rewardDeadlineCount, sample.Sequence, deliveredCount, source, m.lastTimeout.Milliseconds())
	m.startNextEpisodeLocked(sample.Sequence, deliveredCount, sample.DecisionTime)
}

func (m *learningManager) sendStateReportLocked(episode uint32, startTick uint64, reportSeq uint64, reportLength uint64) {
	report := m.metrics.buildReport()
	if report == nil {
		fmt.Printf("[learning] episode %d skipped report: metrics unavailable\n", episode)
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
		fmt.Printf("[learning] SendReport failed: episode=%d target_node=%d err=%v\n", episode, m.nodeID, err)
		return
	}
	fmt.Printf("[learning] sent report: node=%d episode=%d start_tick=%d report_seq=%d window_consensus_count=%d total_transactions=%d throughput_duration_s=%.6f throughput_tps=%.6f\n",
		m.nodeID, episode, startTick, reportSeq, reportLength, reportThroughput.totalTransactions, reportThroughput.duration.Seconds(), report.ThroughputTps)
	if m.pendingReward != nil {
		fmt.Printf("[learning] sent reward: node=%d episode=%d total_transactions=%d throughput_duration_s=%.6f throughput_tps=%.6f\n",
			m.nodeID, m.pendingReward.episode, m.pendingReward.throughputTransactions, m.pendingReward.throughputDuration.Seconds(), m.pendingReward.report.ThroughputTps)
		m.pendingReward = nil
	}
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
			if reportSeq <= startTick {
				continue
			}
			if windowConsensusCount == 0 {
				continue
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
				fmt.Printf("[learning] timeout READY: episode=%d start_tick=%d report_seq=%d window_consensus_count=%d report_end_count=%d timeout_ms=%d\n",
					episode, startTick, reportSeq, windowConsensusCount, reportWindow.endCount, timeout.Milliseconds())
			} else if m.pollerEpisode == episode && m.pollerDecision == nil && !found && !reportedUnknownWindow {
				fmt.Printf("[learning] ignored timeout for unknown local report window: episode=%d start_tick=%d report_seq=%d window_consensus_count=%d\n",
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
	report := m.metrics.buildReport()
	if report == nil {
		return
	}
	throughput := m.metrics.calculateThroughput()
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
