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
	timeout      time.Duration
	startTick    uint64
	reportSeq    uint64
	reportLength uint64
}

type learningEpisodeWindow struct {
	startTick  uint64
	reportSeq  uint64
	reportLen  uint64
	applyTick  uint64
	rewardTick uint64
}

type pendingLearningReward struct {
	report    *adaptivetimers.PbftReport
	episode   uint32
	timeoutMS uint32
}

type learningManager struct {
	lock sync.Mutex

	enabled bool
	nodeID  uint64
	client  *learningAgentClient

	metrics *learningWindowMetrics

	currentEpisode       uint32
	episodeStartTick     uint64
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
	capApplyDeadlineTick       uint64
	capRewardDeadlineTick      uint64
	selectedWindow             *learningEpisodeWindow
	selectedTimeout            time.Duration
	applyHandledForEpisode     bool
	rewardCapturedForEpisode   bool
	waitingForRecommendation   bool

	pollerCancel   context.CancelFunc
	pollerEpisode  uint32
	pollerDecision *learningDecision

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

	m.metrics.record(sample)
	if m.episodeStartWallTime.IsZero() {
		m.episodeStartWallTime = time.Now()
		if sample.Sequence > 0 {
			m.episodeStartTick = sample.Sequence - 1
		} else {
			m.episodeStartTick = 0
		}
	}

	m.maybeConsumeRecommendationLocked(sample.Sequence)
	if sample.Sequence%m.reportTickInterval == 0 {
		m.maybeSendReportTickLocked(sample.Sequence)
	}
	m.maybeHandleApplyDeadlineLocked(sample.Sequence)
	m.maybeHandleRewardDeadlineLocked(sample.Sequence)
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

func (m *learningManager) maybeSendReportTickLocked(sequence uint64) {
	if m.selectedWindow != nil || m.reachedReportCapForEpisode {
		return
	}
	reportLength := sequence - m.episodeStartTick
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
	m.sendStateReportLocked(m.currentEpisode, m.episodeStartTick, reportSeq)
	m.reportSentForEpisode = true
	m.waitingForRecommendation = true
	m.startTimeoutPollingLocked(m.currentEpisode)

	if reportLength >= m.maxReportLength {
		m.reachedReportCapForEpisode = true
		window := newLearningEpisodeWindow(m.episodeStartTick, reportSeq, reportLength)
		m.capApplyDeadlineTick = window.applyTick
		m.capRewardDeadlineTick = window.rewardTick
	}
}

func (m *learningManager) maybeConsumeRecommendationLocked(sequence uint64) {
	if m.selectedWindow != nil || m.pollerDecision == nil || m.pollerEpisode != m.currentEpisode {
		return
	}

	decision := m.pollerDecision
	window := newLearningEpisodeWindow(decision.startTick, decision.reportSeq, decision.reportLength)
	m.selectedWindow = window
	m.selectedTimeout = decision.timeout
	m.waitingForRecommendation = false
	m.stopTimeoutPollingLocked()
	fmt.Printf("[learning] received recommendation: episode=%d report_seq=%d apply_tick=%d reward_tick=%d timeout_ms=%d current_seq=%d\n",
		m.currentEpisode, window.reportSeq, window.applyTick, window.rewardTick, decision.timeout.Milliseconds(), sequence)

	if sequence > window.applyTick {
		m.applyHandledForEpisode = true
		m.lastTimeout = m.currentTimeout
		fmt.Printf("[learning] ignored late recommendation: episode=%d report_seq=%d apply_tick=%d current_seq=%d\n",
			m.currentEpisode, window.reportSeq, window.applyTick, sequence)
	}
}

func (m *learningManager) maybeHandleApplyDeadlineLocked(sequence uint64) {
	if m.applyHandledForEpisode {
		return
	}
	if m.selectedWindow != nil {
		if sequence < m.selectedWindow.applyTick {
			return
		}
		applied := false
		var applyErr error
		recommendedTimeout := m.selectedTimeout
		if sequence == m.selectedWindow.applyTick && recommendedTimeout > 0 {
			if err := m.applyRecommendedTimeoutLocked(recommendedTimeout); err != nil {
				applyErr = err
				fmt.Printf("[learning] failed to apply recommendation: episode=%d report_seq=%d apply_tick=%d current_seq=%d timeout_ms=%d err=%v\n",
					m.currentEpisode, m.selectedWindow.reportSeq, m.selectedWindow.applyTick, sequence, recommendedTimeout.Milliseconds(), err)
			} else {
				applied = true
			}
		}
		m.applyHandledForEpisode = true
		m.lastTimeout = m.currentTimeout
		if applied {
			m.metrics.resetWithThroughputStart(time.Now())
			fmt.Printf("[learning] applied recommendation on time: episode=%d report_seq=%d apply_tick=%d current_seq=%d timeout_ms=%d\n",
				m.currentEpisode, m.selectedWindow.reportSeq, m.selectedWindow.applyTick, sequence, m.currentTimeout.Milliseconds())
		} else if applyErr != nil {
			fmt.Printf("[learning] apply deadline reached with recommendation apply failure: episode=%d report_seq=%d apply_tick=%d current_seq=%d recommended_timeout_ms=%d current_timeout_ms=%d err=%v\n",
				m.currentEpisode, m.selectedWindow.reportSeq, m.selectedWindow.applyTick, sequence, recommendedTimeout.Milliseconds(), m.currentTimeout.Milliseconds(), applyErr)
		} else {
			fmt.Printf("[learning] apply deadline reached without recommendation update: episode=%d report_seq=%d apply_tick=%d current_seq=%d timeout_ms=%d\n",
				m.currentEpisode, m.selectedWindow.reportSeq, m.selectedWindow.applyTick, sequence, m.currentTimeout.Milliseconds())
		}
		return
	}
	if m.reachedReportCapForEpisode && m.capApplyDeadlineTick > 0 && sequence >= m.capApplyDeadlineTick {
		m.waitingForRecommendation = false
		m.applyHandledForEpisode = true
		m.lastTimeout = m.currentTimeout
		m.stopTimeoutPollingLocked()
		fmt.Printf("[learning] recommendation unavailable at cap apply deadline: episode=%d cap_apply_tick=%d current_seq=%d timeout_ms=%d\n",
			m.currentEpisode, m.capApplyDeadlineTick, sequence, m.currentTimeout.Milliseconds())
	}
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

func (m *learningManager) maybeHandleRewardDeadlineLocked(sequence uint64) {
	if m.rewardCapturedForEpisode {
		return
	}
	var rewardDeadline uint64
	source := "none"
	if m.selectedWindow != nil {
		rewardDeadline = m.selectedWindow.rewardTick
		source = "recommended_window"
	} else if m.reachedReportCapForEpisode && m.capRewardDeadlineTick > 0 {
		rewardDeadline = m.capRewardDeadlineTick
		source = "cap_window"
	}
	if rewardDeadline == 0 || sequence < rewardDeadline {
		return
	}

	m.captureRewardLocked(m.currentEpisode)
	m.rewardCapturedForEpisode = true
	m.stopTimeoutPollingLocked()
	fmt.Printf("[learning] captured reward: episode=%d reward_tick=%d current_seq=%d source=%s timeout_ms=%d\n",
		m.currentEpisode, rewardDeadline, sequence, source, m.lastTimeout.Milliseconds())
	m.startNextEpisodeLocked(sequence)
}

func (m *learningManager) sendStateReportLocked(episode uint32, startTick uint64, reportSeq uint64) {
	report := m.metrics.buildReport()
	if report == nil {
		fmt.Printf("[learning] episode %d skipped report: metrics unavailable\n", episode)
		return
	}

	local := &adaptivetimers.ReportLocal{
		NodeId:    uint32(m.nodeID),
		Episode:   episode,
		Protocol:  adaptivetimers.Protocol_PROTOCOL_PBFT,
		StartTick: saturatingUint32(startTick),
		ReportSeq: saturatingUint32(reportSeq),
		State:     &adaptivetimers.ReportLocal_PbftState{PbftState: report},
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
	fmt.Printf("[learning] sent report: node=%d episode=%d start_tick=%d report_seq=%d\n",
		m.nodeID, episode, startTick, reportSeq)
	if m.pendingReward != nil {
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
			if reportSeq <= startTick {
				continue
			}
			timeout := time.Duration(status.Timeout.GetPbft().ElectionTimeoutMilliseconds) * time.Millisecond
			if timeout <= 0 {
				continue
			}

			m.lock.Lock()
			if m.pollerEpisode == episode && m.pollerDecision == nil {
				m.pollerDecision = &learningDecision{
					timeout:      timeout,
					startTick:    startTick,
					reportSeq:    reportSeq,
					reportLength: reportSeq - startTick,
				}
				fmt.Printf("[learning] timeout READY: episode=%d start_tick=%d report_seq=%d report_length=%d timeout_ms=%d\n",
					episode, startTick, reportSeq, reportSeq-startTick, timeout.Milliseconds())
			}
			m.lock.Unlock()
			return
		}
	}
}

func (m *learningManager) captureRewardLocked(episode uint32) {
	report := m.metrics.buildReport()
	if report == nil {
		return
	}
	m.pendingReward = &pendingLearningReward{
		report:    report,
		episode:   episode,
		timeoutMS: uint32(max(int64(0), m.lastTimeout.Milliseconds())),
	}
}

func (m *learningManager) startNextEpisodeLocked(startTick uint64) {
	m.currentEpisode++
	m.episodeStartTick = startTick
	m.episodeStartWallTime = time.Now()
	m.reportSentForEpisode = false
	m.reachedReportCapForEpisode = false
	m.capApplyDeadlineTick = 0
	m.capRewardDeadlineTick = 0
	m.selectedWindow = nil
	m.selectedTimeout = 0
	m.applyHandledForEpisode = false
	m.rewardCapturedForEpisode = false
	m.waitingForRecommendation = false
	m.pollerDecision = nil
	m.pollerEpisode = 0
	m.metrics.reset()
}

func newLearningEpisodeWindow(startTick, reportSeq, reportLength uint64) *learningEpisodeWindow {
	return &learningEpisodeWindow{
		startTick:  startTick,
		reportSeq:  reportSeq,
		reportLen:  reportLength,
		applyTick:  reportSeq + reportLength/2,
		rewardTick: reportSeq + reportLength,
	}
}
