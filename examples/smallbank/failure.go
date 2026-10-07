// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"encoding/xml"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const leaderReplicaToken = "leader"

type proposalDelayController struct {
	enabled     bool
	specPath    string
	startUnixMS int64
	warmUp      time.Duration
	phases      []failurePhase

	lastLoggedPhase atomic.Int64

	lock           sync.Mutex
	pinnedReplicas map[int64]map[uint64]time.Duration
}

type failurePhase struct {
	startOffset time.Duration
	interval    time.Duration
	order       int
	rule        proposalDelayRule
}

type proposalDelayRule struct {
	replicaDelays       map[uint64]time.Duration
	leaderWindowDelay   time.Duration
	hasLeaderWindowRule bool
}

type failureSpecXML struct {
	WarmUpTime   *float64     `xml:"warmUpTime"`
	WarmUpTimeMS *int64       `xml:"warmUpTimeMs"`
	Phases       []failureXML `xml:"phases>phase"`
}

type failureXML struct {
	AtTime *float64    `xml:"atTime"`
	PBFT   failurePBFT `xml:"pbft"`
}

type failurePBFT struct {
	ProposalDelay proposalDelayXML `xml:"proposalDelay"`
}

type proposalDelayXML struct {
	Interval *float64    `xml:"interval"`
	Replicas replicasXML `xml:"replicas"`
}

type replicasXML struct {
	Replica []replicaXML `xml:"replica"`
}

type replicaXML struct {
	ID      string `xml:"id"`
	DelayMS *int64 `xml:"delayMs"`
}

func disabledProposalDelayController() *proposalDelayController {
	c := &proposalDelayController{
		pinnedReplicas: make(map[int64]map[uint64]time.Duration),
	}
	c.lastLoggedPhase.Store(-2)
	return c
}

func loadProposalDelayController(specPath string, startUnixMS int64) (*proposalDelayController, error) {
	if strings.TrimSpace(specPath) == "" {
		return disabledProposalDelayController(), nil
	}
	if startUnixMS < 0 {
		return nil, fmt.Errorf("failure start unix ms must be >= 0")
	}

	raw, err := os.ReadFile(specPath)
	if err != nil {
		return nil, err
	}

	var spec failureSpecXML
	if err := xml.Unmarshal(raw, &spec); err != nil {
		return nil, err
	}

	ctrl := &proposalDelayController{
		enabled:        true,
		specPath:       specPath,
		startUnixMS:    startUnixMS,
		warmUp:         failureWarmUp(spec),
		phases:         make([]failurePhase, 0, len(spec.Phases)),
		pinnedReplicas: make(map[int64]map[uint64]time.Duration),
	}
	ctrl.lastLoggedPhase.Store(-2)

	rawPhases := make([]failurePhaseXML, 0, len(spec.Phases))
	for i, phase := range spec.Phases {
		if phase.AtTime == nil {
			return nil, fmt.Errorf("failure spec %s: phase %d has no <atTime>", specPath, i+1)
		}
		rawPhases = append(rawPhases, failurePhaseXML{
			startOffset:   nonNegativeDurationSeconds(*phase.AtTime),
			order:         i,
			proposalDelay: phase.PBFT.ProposalDelay,
		})
	}
	sort.SliceStable(rawPhases, func(i, j int) bool {
		if rawPhases[i].startOffset == rawPhases[j].startOffset {
			return rawPhases[i].order < rawPhases[j].order
		}
		return rawPhases[i].startOffset < rawPhases[j].startOffset
	})

	for i, phase := range rawPhases {
		var nextStart *time.Duration
		if i+1 < len(rawPhases) {
			nextStart = &rawPhases[i+1].startOffset
		}
		ctrl.phases = appendProposalDelayPhases(ctrl.phases, phase, nextStart)
	}
	sort.SliceStable(ctrl.phases, func(i, j int) bool {
		if ctrl.phases[i].startOffset == ctrl.phases[j].startOffset {
			return ctrl.phases[i].order < ctrl.phases[j].order
		}
		return ctrl.phases[i].startOffset < ctrl.phases[j].startOffset
	})

	return ctrl, nil
}

type failurePhaseXML struct {
	startOffset   time.Duration
	order         int
	proposalDelay proposalDelayXML
}

func appendProposalDelayPhases(phases []failurePhase, phase failurePhaseXML, nextStart *time.Duration) []failurePhase {
	rule := parseProposalDelayRule(phase.proposalDelay)
	interval := proposalDelayInterval(phase.proposalDelay)
	if interval <= 0 {
		return append(phases, failurePhase{
			startOffset: phase.startOffset,
			order:       phase.order,
			rule:        rule,
		})
	}

	maxStart := time.Duration(0)
	hasMaxStart := false
	if nextStart != nil && *nextStart > phase.startOffset {
		maxStart = *nextStart
		hasMaxStart = true
	}

	added := 0
	for start := phase.startOffset; ; start += interval {
		if hasMaxStart && start >= maxStart {
			break
		}
		phases = append(phases, failurePhase{
			startOffset: start,
			interval:    interval,
			order:       phase.order,
			rule:        rule,
		})
		added++
		if !hasMaxStart {
			break
		}
	}

	if added == 0 {
		phases = append(phases, failurePhase{
			startOffset: phase.startOffset,
			order:       phase.order,
			rule:        rule,
		})
	}
	return phases
}

func proposalDelayInterval(delay proposalDelayXML) time.Duration {
	if delay.Interval != nil {
		return nonNegativeDurationSeconds(*delay.Interval)
	}
	return 0
}

func failureWarmUp(spec failureSpecXML) time.Duration {
	if spec.WarmUpTimeMS != nil {
		return nonNegativeDurationMS(*spec.WarmUpTimeMS)
	}
	if spec.WarmUpTime != nil {
		return nonNegativeDurationSeconds(*spec.WarmUpTime)
	}
	return 0
}

func parseProposalDelayRule(delay proposalDelayXML) proposalDelayRule {
	rule := proposalDelayRule{replicaDelays: make(map[uint64]time.Duration)}

	for _, replica := range delay.Replicas.Replica {
		idText := strings.TrimSpace(replica.ID)
		if idText == "" {
			continue
		}

		replicaDelay, hasReplicaDelay := optionalDelayMS(replica.DelayMS)
		if !hasReplicaDelay {
			continue
		}

		if strings.EqualFold(idText, leaderReplicaToken) {
			rule.leaderWindowDelay = replicaDelay
			rule.hasLeaderWindowRule = true
			continue
		}

		replicaID, err := strconv.ParseUint(idText, 10, 64)
		if err == nil {
			rule.replicaDelays[replicaID] = replicaDelay
		}
	}

	return rule
}

func optionalDelayMS(value *int64) (time.Duration, bool) {
	if value == nil {
		return 0, false
	}
	return nonNegativeDurationMS(*value), true
}

func nonNegativeDurationMS(value int64) time.Duration {
	if value < 0 {
		value = 0
	}
	return time.Duration(value) * time.Millisecond
}

func nonNegativeDurationSeconds(value float64) time.Duration {
	if value < 0 {
		value = 0
	}
	return time.Duration(value * float64(time.Second))
}

func (c *proposalDelayController) observeLeader(leaderID uint64, nodeIDs []uint64) {
	if c == nil || !c.enabled || leaderID == 0 {
		return
	}

	elapsedSinceStart := time.Since(time.UnixMilli(c.startUnixMS))
	elapsedSinceWarmUp := elapsedSinceStart - c.warmUp
	activePhase := c.activePhase(elapsedSinceWarmUp)
	c.logPhaseChange(activePhase, elapsedSinceStart, elapsedSinceWarmUp)
	if activePhase < 0 {
		return
	}

	pinKey, intervalTick := c.pinKey(activePhase, elapsedSinceWarmUp)
	c.pinLeaderWindow(activePhase, pinKey, intervalTick, leaderID, nodeIDs)
}

func (c *proposalDelayController) delayForProposal(nodeID uint64) time.Duration {
	if c == nil || !c.enabled {
		return 0
	}

	elapsedSinceStart := time.Since(time.UnixMilli(c.startUnixMS))
	elapsedSinceWarmUp := elapsedSinceStart - c.warmUp
	activePhase := c.activePhase(elapsedSinceWarmUp)
	c.logPhaseChange(activePhase, elapsedSinceStart, elapsedSinceWarmUp)
	if activePhase < 0 {
		return 0
	}

	rule := c.phases[activePhase].rule
	replicaID := smartNodeIDToFailureReplicaID(nodeID)
	if delay, exists := rule.replicaDelays[replicaID]; exists {
		return delay
	}
	if !rule.hasLeaderWindowRule {
		return 0
	}

	pinKey, _ := c.pinKey(activePhase, elapsedSinceWarmUp)
	return c.pinnedLeaderWindow(pinKey)[replicaID]
}

func (c *proposalDelayController) pinKey(activePhase int, elapsedSinceWarmUp time.Duration) (int64, int64) {
	if activePhase < 0 || activePhase >= len(c.phases) {
		return int64(activePhase), 0
	}
	phase := c.phases[activePhase]
	tick := int64(0)
	if phase.interval > 0 && elapsedSinceWarmUp >= phase.startOffset {
		tick = int64((elapsedSinceWarmUp - phase.startOffset) / phase.interval)
	}
	return (int64(activePhase) << 32) | tick, tick
}

func (c *proposalDelayController) activePhase(elapsedSinceWarmUp time.Duration) int {
	if elapsedSinceWarmUp < 0 || len(c.phases) == 0 {
		return -1
	}
	active := -1
	for idx, phase := range c.phases {
		if elapsedSinceWarmUp >= phase.startOffset {
			active = idx
			continue
		}
		break
	}
	return active
}

func (c *proposalDelayController) logPhaseChange(activePhase int, elapsedSinceStart time.Duration, elapsedSinceWarmUp time.Duration) {
	previous := c.lastLoggedPhase.Load()
	if previous == int64(activePhase) {
		return
	}
	if !c.lastLoggedPhase.CompareAndSwap(previous, int64(activePhase)) {
		return
	}
	if activePhase < 0 {
		fmt.Printf("%s injection phase changed: inactive elapsed_since_start_ms=%d elapsed_since_warmup_ms=%d\n",
			timestampedLogTag("failure"), elapsedSinceStart.Milliseconds(), elapsedSinceWarmUp.Milliseconds())
		return
	}
	phase := c.phases[activePhase]
	fmt.Printf("%s injection phase changed: index=%d start_offset_ms=%d elapsed_since_start_ms=%d elapsed_since_warmup_ms=%d\n",
		timestampedLogTag("failure"), activePhase, phase.startOffset.Milliseconds(), elapsedSinceStart.Milliseconds(), elapsedSinceWarmUp.Milliseconds())
}

func (c *proposalDelayController) pinnedLeaderWindow(pinKey int64) map[uint64]time.Duration {
	c.lock.Lock()
	defer c.lock.Unlock()

	pinned := c.pinnedReplicas[pinKey]
	if len(pinned) == 0 {
		return nil
	}
	copied := make(map[uint64]time.Duration, len(pinned))
	for replicaID, delay := range pinned {
		copied[replicaID] = delay
	}
	return copied
}

func (c *proposalDelayController) pinLeaderWindow(activePhase int, pinKey int64, intervalTick int64, leaderID uint64, nodeIDs []uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(c.pinnedReplicas[pinKey]) > 0 {
		return
	}
	if activePhase < 0 || activePhase >= len(c.phases) {
		return
	}

	rule := c.phases[activePhase].rule
	if !rule.hasLeaderWindowRule {
		return
	}

	nodes := append([]uint64(nil), nodeIDs...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	leaderPos := -1
	for i, id := range nodes {
		if id == leaderID {
			leaderPos = i
			break
		}
	}
	if leaderPos < 0 {
		return
	}

	resolved := make(map[uint64]time.Duration)
	windowSize := max(0, min((len(nodes)-1)/3, len(nodes)))
	targets := make([]uint64, 0, windowSize)
	for offset := 0; offset < windowSize; offset++ {
		targetNodeID := nodes[(leaderPos+offset)%len(nodes)]
		replicaID := smartNodeIDToFailureReplicaID(targetNodeID)
		resolved[replicaID] = rule.leaderWindowDelay
		targets = append(targets, replicaID)
	}

	c.pinnedReplicas[pinKey] = resolved
	fmt.Printf("Resolved leader proposal delay window: phase=%d interval_tick=%d leader_replica=%d delay_ms=%d targets=%v\n",
		c.phases[activePhase].order,
		intervalTick,
		smartNodeIDToFailureReplicaID(leaderID),
		rule.leaderWindowDelay.Milliseconds(),
		targets)
}

func smartNodeIDToFailureReplicaID(nodeID uint64) uint64 {
	if nodeID == 0 {
		return 0
	}
	return nodeID - 1
}
