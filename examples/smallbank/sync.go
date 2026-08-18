// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	bft "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"google.golang.org/protobuf/proto"
)

const (
	checkpointPeriod      = 100
	checkpointRetention   = 3
	syncRetryInterval     = 100 * time.Millisecond
	syncDecisionChunkSize = uint64(32)
	syncBulkRPCTimeout    = 5 * time.Second
	syncWallClockBudget   = 10 * time.Second
)

func isCheckpointSequence(sequence uint64) bool {
	return sequence > 0 && sequence%checkpointPeriod == 0
}

// stateCheckpoint is an immutable application image immediately after the
// boundary decision was applied. Sequence zero is the genesis checkpoint and
// intentionally has no decision certificate.
type stateCheckpoint struct {
	Sequence uint64
	View     uint64
	Checksum string
	Accounts []accountSnapshot
	Clients  []clientSnapshot
	Decision bft.Decision
}

type checkpointDescriptor struct {
	Sequence uint64
	View     uint64
	Checksum string
	Decision bft.Decision
}

type loggedDecision struct {
	Sequence uint64
	Decision bft.Decision
}

// stateSyncSnapshot is used for both status and data responses. Header-only
// responses populate the certified frontier, retained checkpoint descriptors,
// and log range. Data responses additionally carry one exact checkpoint and/or
// one exact inclusive decision range.
type stateSyncSnapshot struct {
	NodeID uint64

	LatestSequence uint64
	LatestView     uint64
	Latest         bft.Decision
	LogStart       uint64
	LogEnd         uint64
	Checkpoints    []checkpointDescriptor

	HasCheckpoint bool
	Checkpoint    stateCheckpoint
	Decisions     []bft.Decision
}

type stateSyncVoteKey struct {
	View           uint64
	Sequence       uint64
	Checksum       string
	DecisionDigest string
}

type checkpointCandidate struct {
	descriptor checkpointDescriptor
	sources    []uint64
}

type stateSyncResult struct {
	nodeID   uint64
	snapshot stateSyncSnapshot
	err      error
}

func (n *node) initializeSyncHistory() {
	n.stateLock.Lock()
	image := n.state.deterministicStateSnapshot()
	n.stateLock.Unlock()

	genesis := stateCheckpoint{
		Sequence: 0,
		View:     0,
		Checksum: hashBytes(mustJSON(stateSnapshotImage(image.Accounts, image.Clients))),
		Accounts: cloneAccountSnapshots(image.Accounts),
		Clients:  cloneClientSnapshots(image.Clients),
	}

	n.historyLock.Lock()
	n.checkpoints = []stateCheckpoint{genesis}
	n.decisionLog = nil
	n.historyLatestView = 0
	n.historyLatestSequence = 0
	n.historyLatestDecision = bft.Decision{}
	n.historyLock.Unlock()
}

// appendHistoryLocked advances the sync frontier and retained data alongside
// the application transition. The caller holds stateLock, lastLock, and
// historyLock and has already verified sequence continuity.
func (n *node) appendHistoryLocked(md *smartbftprotos.ViewMetadata, decision bft.Decision, image *stateSnapshot) {
	sequence := md.GetLatestSequence()

	n.decisionLog = append(n.decisionLog, loggedDecision{Sequence: sequence, Decision: decision})
	n.historyLatestView = md.GetViewId()
	n.historyLatestSequence = sequence
	n.historyLatestDecision = decision

	if image == nil {
		return
	}

	n.checkpoints = append(n.checkpoints, stateCheckpoint{
		Sequence: sequence,
		View:     md.GetViewId(),
		Checksum: hashBytes(mustJSON(stateSnapshotImage(image.Accounts, image.Clients))),
		Accounts: cloneAccountSnapshots(image.Accounts),
		Clients:  cloneClientSnapshots(image.Clients),
		Decision: decision,
	})
	if len(n.checkpoints) <= checkpointRetention {
		return
	}

	n.checkpoints = append([]stateCheckpoint(nil), n.checkpoints[len(n.checkpoints)-checkpointRetention:]...)
	oldest := n.checkpoints[0].Sequence
	firstRetained := 0
	for firstRetained < len(n.decisionLog) && n.decisionLog[firstRetained].Sequence <= oldest {
		firstRetained++
	}
	n.decisionLog = append([]loggedDecision(nil), n.decisionLog[firstRetained:]...)
}

func cloneCheckpoint(checkpoint stateCheckpoint, withState bool) stateCheckpoint {
	cloned := stateCheckpoint{
		Sequence: checkpoint.Sequence,
		View:     checkpoint.View,
		Checksum: checkpoint.Checksum,
		Decision: cloneDecision(checkpoint.Decision),
	}
	if withState {
		cloned.Accounts = cloneAccountSnapshots(checkpoint.Accounts)
		cloned.Clients = cloneClientSnapshots(checkpoint.Clients)
	}
	return cloned
}

func checkpointDescription(checkpoint stateCheckpoint) checkpointDescriptor {
	return checkpointDescriptor{
		Sequence: checkpoint.Sequence,
		View:     checkpoint.View,
		Checksum: checkpoint.Checksum,
		Decision: cloneDecision(checkpoint.Decision),
	}
}

func (n *node) syncHeader() stateSyncSnapshot {
	n.historyLock.Lock()
	defer n.historyLock.Unlock()

	header := stateSyncSnapshot{
		NodeID:         n.id,
		LatestSequence: n.historyLatestSequence,
		LatestView:     n.historyLatestView,
		Latest:         cloneDecision(n.historyLatestDecision),
		LogEnd:         n.historyLatestSequence,
	}
	if len(n.checkpoints) > 0 {
		header.LogStart = n.checkpoints[0].Sequence + 1
		header.Checkpoints = make([]checkpointDescriptor, 0, len(n.checkpoints))
		for _, checkpoint := range n.checkpoints {
			header.Checkpoints = append(header.Checkpoints, checkpointDescription(checkpoint))
		}
	} else {
		header.LogStart = n.historyLatestSequence + 1
	}
	return header
}

// serveSyncSnapshot resolves checkpoint and range requests against one locked
// history generation. Requests name exact immutable objects, so advancing past
// another boundary cannot silently substitute a newer checkpoint or tail.
func (n *node) serveSyncSnapshot(req grpcStateSnapshotRequest) (stateSyncSnapshot, error) {
	n.historyLock.Lock()
	response := stateSyncSnapshot{
		NodeID:         n.id,
		LatestSequence: n.historyLatestSequence,
		LatestView:     n.historyLatestView,
		LogEnd:         n.historyLatestSequence,
	}
	latest := n.historyLatestDecision
	if len(n.checkpoints) > 0 {
		response.LogStart = n.checkpoints[0].Sequence + 1
	} else {
		response.LogStart = n.historyLatestSequence + 1
	}

	if req.HeaderOnly {
		checkpoints := append([]stateCheckpoint(nil), n.checkpoints...)
		n.historyLock.Unlock()
		response.Latest = cloneDecision(latest)
		response.Checkpoints = make([]checkpointDescriptor, 0, len(checkpoints))
		for _, checkpoint := range checkpoints {
			response.Checkpoints = append(response.Checkpoints, checkpointDescription(checkpoint))
		}
		return response, nil
	}

	var selectedCheckpoint stateCheckpoint
	if req.WantCheckpoint {
		found := false
		for _, checkpoint := range n.checkpoints {
			if checkpoint.Sequence != req.CheckpointSequence || checkpoint.Checksum != req.CheckpointChecksum {
				continue
			}
			selectedCheckpoint = checkpoint
			found = true
			break
		}
		if !found {
			n.historyLock.Unlock()
			return stateSyncSnapshot{}, fmt.Errorf("checkpoint seq=%d checksum=%s is not retained", req.CheckpointSequence, req.CheckpointChecksum)
		}
	}

	if req.FromSequence == 0 && req.ThroughSequence == 0 {
		n.historyLock.Unlock()
		response.Latest = cloneDecision(latest)
		if req.WantCheckpoint {
			response.HasCheckpoint = true
			response.Checkpoint = cloneCheckpoint(selectedCheckpoint, true)
		}
		return response, nil
	}
	if req.FromSequence == 0 || req.ThroughSequence < req.FromSequence {
		n.historyLock.Unlock()
		return stateSyncSnapshot{}, fmt.Errorf("invalid decision range [%d,%d]", req.FromSequence, req.ThroughSequence)
	}
	if req.FromSequence < response.LogStart || req.ThroughSequence > response.LogEnd {
		n.historyLock.Unlock()
		return stateSyncSnapshot{}, fmt.Errorf("decision range [%d,%d] unavailable; retained=[%d,%d]", req.FromSequence, req.ThroughSequence, response.LogStart, response.LogEnd)
	}

	expected := req.FromSequence
	entries := make([]loggedDecision, 0, req.ThroughSequence-req.FromSequence+1)
	for _, entry := range n.decisionLog {
		if entry.Sequence < req.FromSequence {
			continue
		}
		if entry.Sequence > req.ThroughSequence {
			break
		}
		if entry.Sequence != expected {
			n.historyLock.Unlock()
			return stateSyncSnapshot{}, fmt.Errorf("local decision log gap: expected %d, got %d", expected, entry.Sequence)
		}
		entries = append(entries, entry)
		expected++
	}
	if expected != req.ThroughSequence+1 {
		n.historyLock.Unlock()
		return stateSyncSnapshot{}, fmt.Errorf("local decision log ended at %d, expected %d", expected-1, req.ThroughSequence)
	}
	n.historyLock.Unlock()

	response.Latest = cloneDecision(latest)
	if req.WantCheckpoint {
		response.HasCheckpoint = true
		response.Checkpoint = cloneCheckpoint(selectedCheckpoint, true)
	}
	response.Decisions = make([]bft.Decision, 0, len(entries))
	for _, entry := range entries {
		response.Decisions = append(response.Decisions, cloneDecision(entry.Decision))
	}
	return response, nil
}

func (n *node) Sync() bft.SyncResponse {
	start := time.Now()
	defer func() {
		smallbankTracePrintf("%s app sync: node=%d event=done elapsed_ms=%d\n", timestampedLogTag("sync"), n.id, time.Since(start).Milliseconds())
	}()

	if n.network == nil {
		return bft.SyncResponse{Latest: n.currentDecision()}
	}
	ctx, cancel := context.WithTimeout(context.Background(), syncWallClockBudget)
	defer cancel()
	go func() {
		select {
		case <-n.stopChan:
			cancel()
		case <-ctx.Done():
		}
	}()

	for {
		response, complete := n.checkpointSync(ctx)
		if complete {
			return response
		}
		select {
		case <-ctx.Done():
			smallbankTracePrintf("%s app sync: node=%d event=budget_exhausted elapsed_ms=%d position=%d err=%v\n",
				timestampedLogTag("sync"), n.id, time.Since(start).Milliseconds(), n.currentSequence(), ctx.Err())
			return bft.SyncResponse{Latest: n.currentDecision()}
		case <-time.After(syncRetryInterval):
		}
	}
}

// checkpointSync performs one recovery round against a fixed, certified target.
// A false result means the caller must rediscover because retained data or peer
// availability changed during the round.
func (n *node) checkpointSync(ctx context.Context) (bft.SyncResponse, bool) {
	local := n.syncHeader()
	smallbankTracePrintf("%s app sync: node=%d event=start local_seq=%d local_view=%d checkpoints=%d network=%t\n",
		timestampedLogTag("sync"), n.id, local.LatestSequence, local.LatestView, len(local.Checkpoints), n.network != nil)

	quorum, _ := decisionQuorum(len(n.Nodes()))
	peerHeaders := n.fetchSyncHeaders(ctx)
	headers := append(peerHeaders, local)
	if len(headers) < quorum {
		smallbankTracePrintf("%s app sync: node=%d event=insufficient_headers received=%d required=%d\n",
			timestampedLogTag("sync"), n.id, len(headers), quorum)
		return bft.SyncResponse{Latest: n.currentDecision()}, false
	}

	requiredCheckpointVotes := stateSyncMatchCount(len(n.Nodes()))
	candidate, hasCandidate := selectCheckpointCandidate(headers, requiredCheckpointVotes)
	if local.LatestSequence == 0 && !hasCandidate {
		smallbankTracePrintf("%s app sync: node=%d event=genesis_agreement_missing headers=%d required=%d\n",
			timestampedLogTag("sync"), n.id, len(headers), requiredCheckpointVotes)
		return bft.SyncResponse{Latest: n.currentDecision()}, false
	}
	position := local.LatestSequence
	if hasCandidate && candidate.descriptor.Sequence > position {
		position = candidate.descriptor.Sequence
	}

	target := position
	donors := []uint64(nil)
	if suffixTarget, suffixDonors, ok := selectSyncTarget(headers, position+1, requiredCheckpointVotes); ok {
		target = suffixTarget
		donors = suffixDonors
	} else if position == local.LatestSequence && countHeadersAhead(headers, position) >= requiredCheckpointVotes {
		// A quorum subset reports newer state, but this round did not find a
		// common retained checkpoint or suffix. Rediscover instead of declaring
		// the stale local position synchronized.
		return bft.SyncResponse{Latest: n.currentDecision()}, false
	}
	checkpointSequence := uint64(0)
	if hasCandidate {
		checkpointSequence = candidate.descriptor.Sequence
	}
	smallbankTracePrintf("%s app sync: node=%d event=target_selected checkpoint=%d target=%d donors=%v\n",
		timestampedLogTag("sync"), n.id, checkpointSequence, target, donors)

	actualPosition := local.LatestSequence
	installCandidate := hasCandidate && checkpointNeedsInstall(local, candidate.descriptor)
	if installCandidate {
		if err := n.installAgreedCheckpoint(ctx, candidate); err != nil {
			smallbankTracePrintf("%s app sync: node=%d event=checkpoint_failed seq=%d err=%v\n",
				timestampedLogTag("sync"), n.id, candidate.descriptor.Sequence, err)
			return bft.SyncResponse{Latest: n.currentDecision()}, false
		}
		actualPosition = candidate.descriptor.Sequence
	}

	if actualPosition < target {
		if err := n.catchUpTo(ctx, headers, donors, actualPosition, target); err != nil {
			smallbankTracePrintf("%s app sync: node=%d event=replay_incomplete position=%d target=%d err=%v\n",
				timestampedLogTag("sync"), n.id, n.currentSequence(), target, err)
			return bft.SyncResponse{Latest: n.currentDecision()}, false
		}
	}
	if n.currentSequence() != target {
		smallbankTracePrintf("%s app sync: node=%d event=target_mismatch position=%d target=%d\n",
			timestampedLogTag("sync"), n.id, n.currentSequence(), target)
		return bft.SyncResponse{Latest: n.currentDecision()}, false
	}

	if target > local.LatestSequence {
		n.pruneExecutedFromPool()
	}
	smallbankTracePrintf("%s app sync: node=%d event=target_reached seq=%d\n", timestampedLogTag("sync"), n.id, target)
	return bft.SyncResponse{Latest: n.currentDecision()}, true
}

func countHeadersAhead(headers []stateSyncSnapshot, sequence uint64) int {
	count := 0
	for _, header := range headers {
		if header.LatestSequence > sequence {
			count++
		}
	}
	return count
}

func (n *node) fetchSyncHeaders(ctx context.Context) []stateSyncSnapshot {
	headerCtx, cancel := context.WithTimeout(ctx, defaultNetworkSendTimeout+250*time.Millisecond)
	defer cancel()
	results := make(chan stateSyncResult, len(n.Nodes()))
	requests := 0
	for _, id := range n.Nodes() {
		if id == n.id {
			continue
		}
		requests++
		go func(peerID uint64) {
			snapshot, err := n.network.fetchStateSnapshot(headerCtx, peerID, grpcStateSnapshotRequest{HeaderOnly: true}, defaultNetworkSendTimeout)
			results <- stateSyncResult{nodeID: peerID, snapshot: snapshot, err: err}
		}(id)
	}

	headers := make([]stateSyncSnapshot, 0, requests)
	for received := 0; received < requests; {
		select {
		case result := <-results:
			received++
			if result.err != nil {
				smallbankTracePrintf("%s app sync: node=%d event=header_failed peer=%d err=%v\n", timestampedLogTag("sync"), n.id, result.nodeID, result.err)
				continue
			}
			if result.snapshot.NodeID != result.nodeID {
				smallbankTracePrintf("%s app sync: node=%d event=header_identity_mismatch peer=%d claimed=%d\n", timestampedLogTag("sync"), n.id, result.nodeID, result.snapshot.NodeID)
				continue
			}
			if err := n.validateSyncHeader(result.snapshot); err != nil {
				smallbankTracePrintf("%s app sync: node=%d event=header_invalid peer=%d err=%v\n", timestampedLogTag("sync"), n.id, result.nodeID, err)
				continue
			}
			headers = append(headers, result.snapshot)
		case <-headerCtx.Done():
			return headers
		}
	}
	return headers
}

func (n *node) validateSyncHeader(header stateSyncSnapshot) error {
	if !n.isMember(header.NodeID) {
		return fmt.Errorf("unknown node ID %d", header.NodeID)
	}
	if header.LogEnd != header.LatestSequence {
		return fmt.Errorf("log end %d does not match latest sequence %d", header.LogEnd, header.LatestSequence)
	}
	if header.LogStart == 0 || header.LogStart > header.LogEnd+1 {
		return fmt.Errorf("invalid retained range [%d,%d]", header.LogStart, header.LogEnd)
	}
	if header.LatestSequence == 0 {
		if len(header.Latest.Proposal.Metadata) != 0 {
			return fmt.Errorf("genesis frontier has a decision")
		}
	} else {
		view, sequence, ok, err := decisionViewSequence(header.Latest)
		if err != nil {
			return fmt.Errorf("latest decision metadata: %w", err)
		}
		if !ok {
			return fmt.Errorf("latest decision has no metadata")
		}
		if sequence != header.LatestSequence || view != header.LatestView {
			return fmt.Errorf("latest decision is view=%d seq=%d, header is view=%d seq=%d", view, sequence, header.LatestView, header.LatestSequence)
		}
		if err := n.verifyDecisionCertificate(header.Latest); err != nil {
			return fmt.Errorf("latest decision certificate: %w", err)
		}
	}

	var previous uint64
	for i, checkpoint := range header.Checkpoints {
		if i > 0 && checkpoint.Sequence <= previous {
			return fmt.Errorf("checkpoint descriptors are not strictly increasing")
		}
		previous = checkpoint.Sequence
		if checkpoint.Sequence > header.LatestSequence {
			return fmt.Errorf("checkpoint %d is beyond latest sequence %d", checkpoint.Sequence, header.LatestSequence)
		}
		if checkpoint.Checksum == "" {
			return fmt.Errorf("checkpoint %d has an empty checksum", checkpoint.Sequence)
		}
		if err := n.validateCheckpointDescriptor(checkpoint); err != nil {
			return err
		}
	}
	if len(header.Checkpoints) == 0 {
		return fmt.Errorf("no retained checkpoint")
	}
	if header.LogStart != header.Checkpoints[0].Sequence+1 {
		return fmt.Errorf("log starts at %d but oldest checkpoint is %d", header.LogStart, header.Checkpoints[0].Sequence)
	}
	return nil
}

func (n *node) validateCheckpointDescriptor(checkpoint checkpointDescriptor) error {
	if checkpoint.Sequence == 0 {
		if checkpoint.View != 0 || len(checkpoint.Decision.Proposal.Metadata) != 0 {
			return fmt.Errorf("invalid genesis checkpoint")
		}
		return nil
	}
	if !isCheckpointSequence(checkpoint.Sequence) {
		return fmt.Errorf("sequence %d is not a checkpoint boundary", checkpoint.Sequence)
	}
	view, sequence, ok, err := decisionViewSequence(checkpoint.Decision)
	if err != nil {
		return fmt.Errorf("checkpoint %d decision metadata: %w", checkpoint.Sequence, err)
	}
	if !ok {
		return fmt.Errorf("checkpoint %d has no decision metadata", checkpoint.Sequence)
	}
	if view != checkpoint.View || sequence != checkpoint.Sequence {
		return fmt.Errorf("checkpoint decision is view=%d seq=%d, descriptor is view=%d seq=%d", view, sequence, checkpoint.View, checkpoint.Sequence)
	}
	if err := n.verifyDecisionCertificate(checkpoint.Decision); err != nil {
		return fmt.Errorf("checkpoint %d certificate: %w", checkpoint.Sequence, err)
	}
	return nil
}

func selectCheckpointCandidate(headers []stateSyncSnapshot, required int) (checkpointCandidate, bool) {
	type accumulated struct {
		descriptor checkpointDescriptor
		sources    map[uint64]struct{}
	}
	candidates := make(map[stateSyncVoteKey]*accumulated)
	for _, header := range headers {
		seen := make(map[stateSyncVoteKey]struct{})
		for _, descriptor := range header.Checkpoints {
			key := checkpointKey(descriptor)
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			candidate := candidates[key]
			if candidate == nil {
				candidate = &accumulated{descriptor: descriptor, sources: make(map[uint64]struct{})}
				candidates[key] = candidate
			}
			candidate.sources[header.NodeID] = struct{}{}
		}
	}

	var best checkpointCandidate
	found := false
	for _, candidate := range candidates {
		if len(candidate.sources) < required {
			continue
		}
		if found && candidate.descriptor.Sequence < best.descriptor.Sequence {
			continue
		}
		sources := make([]uint64, 0, len(candidate.sources))
		for source := range candidate.sources {
			sources = append(sources, source)
		}
		sort.Slice(sources, func(i, j int) bool { return sources[i] < sources[j] })
		best = checkpointCandidate{descriptor: candidate.descriptor, sources: sources}
		found = true
	}
	return best, found
}

// selectSyncTarget returns the greatest fixed target available from at least
// required distinct replicas. The f+1-th highest retained frontier prevents one
// faulty peer from making the target arbitrarily large.
func selectSyncTarget(headers []stateSyncSnapshot, fromSequence uint64, required int) (uint64, []uint64, bool) {
	if fromSequence == 0 {
		return 0, nil, false
	}
	candidates := make([]uint64, 0, len(headers))
	for _, header := range headers {
		if header.LatestSequence >= fromSequence && header.LogStart <= fromSequence {
			candidates = append(candidates, header.LatestSequence)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i] > candidates[j] })
	for _, target := range candidates {
		donors := make([]uint64, 0, len(headers))
		seen := make(map[uint64]struct{})
		for _, header := range headers {
			if header.LogStart > fromSequence || header.LogEnd < target {
				continue
			}
			if _, duplicate := seen[header.NodeID]; duplicate {
				continue
			}
			seen[header.NodeID] = struct{}{}
			donors = append(donors, header.NodeID)
		}
		if len(donors) >= required {
			sort.Slice(donors, func(i, j int) bool { return donors[i] < donors[j] })
			return target, donors, true
		}
	}
	return 0, nil, false
}

func (n *node) installAgreedCheckpoint(ctx context.Context, candidate checkpointCandidate) error {
	var lastErr error
	for _, source := range candidate.sources {
		if err := ctx.Err(); err != nil {
			return err
		}
		if source == n.id {
			continue
		}
		payload, err := n.network.fetchStateSnapshot(ctx, source, grpcStateSnapshotRequest{
			WantCheckpoint:     true,
			CheckpointSequence: candidate.descriptor.Sequence,
			CheckpointChecksum: candidate.descriptor.Checksum,
		}, syncBulkRPCTimeout)
		if err != nil {
			lastErr = err
			continue
		}
		if !payload.HasCheckpoint {
			lastErr = fmt.Errorf("peer %d returned no checkpoint", source)
			continue
		}
		if !sameCheckpointDescriptor(checkpointDescription(payload.Checkpoint), candidate.descriptor) {
			lastErr = fmt.Errorf("peer %d returned a different checkpoint", source)
			continue
		}
		if err := n.validateCheckpointState(payload.Checkpoint); err != nil {
			lastErr = fmt.Errorf("peer %d checkpoint: %w", source, err)
			continue
		}
		if err := n.installStateCheckpoint(payload.Checkpoint); err != nil {
			lastErr = err
			continue
		}
		smallbankTracePrintf("%s app sync: node=%d event=checkpoint_installed source=%d seq=%d checksum=%s\n",
			timestampedLogTag("sync"), n.id, source, payload.Checkpoint.Sequence, payload.Checkpoint.Checksum)
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no remote source for checkpoint %d", candidate.descriptor.Sequence)
	}
	return lastErr
}

func (n *node) catchUpTo(ctx context.Context, headers []stateSyncSnapshot, donors []uint64, position uint64, target uint64) error {
	eligible := make(map[uint64]stateSyncSnapshot)
	for _, header := range headers {
		eligible[header.NodeID] = header
	}

	for position < target {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunkThrough := nextSyncChunkThrough(position, target)
		madeProgress := false
		var lastErr error
		for _, source := range donors {
			if err := ctx.Err(); err != nil {
				return err
			}
			if source == n.id {
				continue
			}
			header, exists := eligible[source]
			if !exists || header.LogStart > position+1 || header.LogEnd < chunkThrough {
				continue
			}
			payload, err := n.network.fetchStateSnapshot(ctx, source, grpcStateSnapshotRequest{
				FromSequence:    position + 1,
				ThroughSequence: chunkThrough,
			}, syncBulkRPCTimeout)
			if err != nil {
				lastErr = err
				continue
			}
			applied, err := n.replayDecisions(payload.Decisions, position, chunkThrough)
			position += uint64(applied)
			if applied > 0 {
				madeProgress = true
			}
			if position == chunkThrough {
				madeProgress = true
				break
			}
			if err != nil {
				lastErr = fmt.Errorf("peer %d: %w", source, err)
			}
			if madeProgress {
				break
			}
		}
		if !madeProgress {
			if lastErr == nil {
				lastErr = fmt.Errorf("no donor retained chunk [%d,%d] toward target %d", position+1, chunkThrough, target)
			}
			return lastErr
		}
	}
	return nil
}

func nextSyncChunkThrough(position uint64, target uint64) uint64 {
	remaining := target - position
	if remaining <= syncDecisionChunkSize {
		return target
	}
	return position + syncDecisionChunkSize
}

// replayDecisions validates and applies an exact ordered prefix. Returning a
// positive count with an error is intentional: the verified prefix remains safe
// and the caller continues from it using another donor rather than declaring the
// synchronization complete.
func (n *node) replayDecisions(decisions []bft.Decision, position uint64, through uint64) (int, error) {
	if through < position {
		return 0, fmt.Errorf("target %d is behind position %d", through, position)
	}
	expected := position + 1
	applied := 0
	for _, decision := range decisions {
		view, sequence, ok, err := decisionViewSequence(decision)
		if err != nil {
			return applied, err
		}
		if !ok {
			return applied, fmt.Errorf("decision without metadata at sequence %d", expected)
		}
		if sequence != expected {
			return applied, fmt.Errorf("decision sequence gap: expected %d, got %d", expected, sequence)
		}
		if sequence > through {
			return applied, fmt.Errorf("decision %d exceeds fixed target %d", sequence, through)
		}
		if err := n.verifyDecisionCertificate(decision); err != nil {
			return applied, fmt.Errorf("verify decision %d in view %d: %w", sequence, view, err)
		}
		if _, err := n.applyDecision(decision.Proposal, decision.Signatures); err != nil {
			return applied, fmt.Errorf("apply decision %d: %w", sequence, err)
		}
		applied++
		expected++
	}
	if expected != through+1 {
		return applied, fmt.Errorf("decision range ended at %d, expected target %d", expected-1, through)
	}
	return applied, nil
}

// verifyDecisionCertificate checks exactly the invariants relied upon by live
// consensus: a valid proposal and a commit quorum over that proposal. Sequence
// continuity is checked by replayDecisions. PrevHash is deliberately not
// checked here because the current live VerifyProposal path does not enforce it.
func (n *node) verifyDecisionCertificate(decision bft.Decision) error {
	if _, err := n.VerifyProposal(decision.Proposal); err != nil {
		return err
	}

	quorum, _ := decisionQuorum(len(n.Nodes()))
	signers := make(map[uint64]struct{}, len(decision.Signatures))
	for _, signature := range decision.Signatures {
		if !n.isMember(signature.ID) {
			return fmt.Errorf("signature from non-member %d", signature.ID)
		}
		if _, duplicate := signers[signature.ID]; duplicate {
			continue
		}
		if _, err := n.VerifyConsenterSig(signature, decision.Proposal); err != nil {
			return fmt.Errorf("signature from %d: %w", signature.ID, err)
		}
		signers[signature.ID] = struct{}{}
	}
	if len(signers) < quorum {
		return fmt.Errorf("only %d valid member signatures, need %d", len(signers), quorum)
	}
	return nil
}

func (n *node) isMember(id uint64) bool {
	for _, member := range n.Nodes() {
		if member == id {
			return true
		}
	}
	return false
}

func (n *node) currentSequence() uint64 {
	n.historyLock.Lock()
	defer n.historyLock.Unlock()
	return n.historyLatestSequence
}

func (n *node) currentDecision() bft.Decision {
	n.historyLock.Lock()
	defer n.historyLock.Unlock()
	return cloneDecision(n.historyLatestDecision)
}

func (n *node) validateCheckpointState(checkpoint stateCheckpoint) error {
	descriptor := checkpointDescription(checkpoint)
	if err := n.validateCheckpointDescriptor(descriptor); err != nil {
		return err
	}
	if checkpoint.Checksum != hashBytes(mustJSON(stateSnapshotImage(checkpoint.Accounts, checkpoint.Clients))) {
		return fmt.Errorf("checkpoint checksum mismatch")
	}
	if _, err := stateFromSnapshots(checkpoint.Accounts, checkpoint.Clients); err != nil {
		return err
	}
	return nil
}

func (n *node) installStateCheckpoint(checkpoint stateCheckpoint) error {
	if err := n.validateCheckpointState(checkpoint); err != nil {
		return err
	}
	state, err := stateFromSnapshots(checkpoint.Accounts, checkpoint.Clients)
	if err != nil {
		return err
	}
	installed := cloneCheckpoint(checkpoint, true)

	n.stateLock.Lock()
	defer n.stateLock.Unlock()
	n.lastLock.Lock()
	defer n.lastLock.Unlock()
	n.historyLock.Lock()
	defer n.historyLock.Unlock()

	state.duplicatesSkipped = n.state.duplicatesSkipped
	n.state = state
	if checkpoint.Sequence == 0 {
		n.prevHash = ""
		n.lastDelivered = false
		n.lastView = 0
		n.lastIndex = 0
		n.lastDecision = bft.Decision{}
	} else {
		n.prevHash = installed.Decision.Proposal.Digest()
		n.lastDelivered = true
		n.lastView = installed.View
		n.lastIndex = installed.Sequence
		n.lastDecision = installed.Decision
	}
	n.lastLeaderID = n.leaderID()

	n.checkpoints = []stateCheckpoint{installed}
	n.decisionLog = nil
	n.historyLatestView = installed.View
	n.historyLatestSequence = installed.Sequence
	n.historyLatestDecision = installed.Decision
	return nil
}

func stateSnapshotImage(accounts []accountSnapshot, clients []clientSnapshot) stateSnapshot {
	if accounts == nil {
		accounts = []accountSnapshot{}
	}
	if clients == nil {
		clients = []clientSnapshot{}
	}
	return stateSnapshot{Accounts: accounts, Clients: clients}
}

func checkpointKey(checkpoint checkpointDescriptor) stateSyncVoteKey {
	digest := ""
	if len(checkpoint.Decision.Proposal.Metadata) > 0 {
		digest = checkpoint.Decision.Proposal.Digest()
	}
	return stateSyncVoteKey{
		View:           checkpoint.View,
		Sequence:       checkpoint.Sequence,
		Checksum:       checkpoint.Checksum,
		DecisionDigest: digest,
	}
}

func sameCheckpointDescriptor(a, b checkpointDescriptor) bool {
	return checkpointKey(a) == checkpointKey(b)
}

func headerContainsCheckpoint(header stateSyncSnapshot, descriptor checkpointDescriptor) bool {
	for _, checkpoint := range header.Checkpoints {
		if sameCheckpointDescriptor(checkpoint, descriptor) {
			return true
		}
	}
	return false
}

func checkpointNeedsInstall(local stateSyncSnapshot, descriptor checkpointDescriptor) bool {
	if descriptor.Sequence > local.LatestSequence {
		return true
	}
	return descriptor.Sequence == 0 && local.LatestSequence == 0 &&
		!headerContainsCheckpoint(local, descriptor)
}

func stateSyncMatchCount(nodeCount int) int {
	if nodeCount <= 1 {
		return 1
	}
	f := (nodeCount - 1) / 3
	return f + 1
}

func decisionQuorum(nodeCount int) (int, int) {
	if nodeCount <= 1 {
		return 1, 0
	}
	f := (nodeCount - 1) / 3
	q := (nodeCount + f + 2) / 2
	return q, f
}

func decisionViewSequence(decision bft.Decision) (uint64, uint64, bool, error) {
	if len(decision.Proposal.Metadata) == 0 {
		return 0, 0, false, nil
	}
	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(decision.Proposal.Metadata, md); err != nil {
		return 0, 0, false, fmt.Errorf("unmarshal decision metadata: %w", err)
	}
	return md.GetViewId(), md.GetLatestSequence(), true, nil
}

func stateFromSnapshots(accounts []accountSnapshot, clients []clientSnapshot) (*smallBankState, error) {
	state := newSmallBankState()
	var previous uint64
	for i, account := range accounts {
		if i > 0 && account.CustomerID <= previous {
			return nil, fmt.Errorf("account snapshot is not strictly sorted at index %d", i)
		}
		previous = account.CustomerID
		state.accounts[account.CustomerID] = account.CustomerName
		state.checking[account.CustomerID] = account.CheckingBalanceCents
		state.savings[account.CustomerID] = account.SavingsBalanceCents
	}

	var previousClient string
	for i, client := range clients {
		if client.ClientID == "" {
			return nil, fmt.Errorf("client snapshot has empty client id at index %d", i)
		}
		if i > 0 && client.ClientID <= previousClient {
			return nil, fmt.Errorf("client snapshot is not strictly sorted at index %d", i)
		}
		previousClient = client.ClientID
		state.lastExecuted[client.ClientID] = client.LastID
		if client.Response.ID != "" {
			state.lastResponse[client.ClientID] = client.Response
		}
	}
	return state, nil
}

func cloneAccountSnapshots(accounts []accountSnapshot) []accountSnapshot {
	if len(accounts) == 0 {
		return nil
	}
	cloned := append([]accountSnapshot(nil), accounts...)
	sort.Slice(cloned, func(i, j int) bool { return cloned[i].CustomerID < cloned[j].CustomerID })
	return cloned
}

func cloneClientSnapshots(clients []clientSnapshot) []clientSnapshot {
	if len(clients) == 0 {
		return nil
	}
	cloned := append([]clientSnapshot(nil), clients...)
	sort.Slice(cloned, func(i, j int) bool { return cloned[i].ClientID < cloned[j].ClientID })
	return cloned
}

func cloneDecision(decision bft.Decision) bft.Decision {
	return bft.Decision{
		Proposal:   cloneProposal(decision.Proposal),
		Signatures: cloneSignatures(decision.Signatures),
	}
}
