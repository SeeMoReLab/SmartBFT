// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"fmt"
	"reflect"
	"testing"

	algorithm "github.com/hyperledger-labs/SmartBFT/internal/bft"
	bft "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"google.golang.org/protobuf/proto"
)

func checkpointTestNode(t *testing.T, id uint64) *node {
	t.Helper()
	out := make(map[uint64]chan<- wireMessage)
	for _, peer := range []uint64{1, 2, 3, 4} {
		if peer != id {
			out[peer] = nil
		}
	}
	n := &node{
		id:      id,
		out:     out,
		state:   newSmallBankState(),
		pending: newPendingTracker(),
		logger:  &testLogger{t: t},
	}
	seedAccounts(t, n.state)
	n.initializeSyncHistory()
	return n
}

type testLogger struct{ t *testing.T }

func (l *testLogger) Debugf(string, ...interface{})     {}
func (l *testLogger) Infof(string, ...interface{})      {}
func (l *testLogger) Errorf(string, ...interface{})     {}
func (l *testLogger) Warnf(string, ...interface{})      {}
func (l *testLogger) Panicf(f string, a ...interface{}) { l.t.Fatalf(f, a...) }

func buildDecision(t *testing.T, seq uint64, view uint64, prevHash string, reqs []request, signers []uint64) bft.Decision {
	t.Helper()
	raws := make([][]byte, 0, len(reqs))
	for _, req := range reqs {
		raw, err := encodeRequest(req)
		if err != nil {
			t.Fatalf("encode request: %v", err)
		}
		raws = append(raws, raw)
	}
	payload := encodeBlockData(blockData{Requests: raws})
	metadata, err := proto.Marshal(&smartbftprotos.ViewMetadata{ViewId: view, LatestSequence: seq})
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	proposal := bft.Proposal{
		Header: encodeBlockHeader(blockHeader{
			PrevHash: prevHash,
			DataHash: hashBytes(payload),
			Sequence: int64(seq),
		}),
		Payload:  payload,
		Metadata: metadata,
	}
	signatures := make([]bft.Signature, 0, len(signers))
	for _, id := range signers {
		signatures = append(signatures, bft.Signature{ID: id, Msg: []byte("msg"), Value: []byte("sig")})
	}
	return bft.Decision{Proposal: proposal, Signatures: signatures}
}

func deliverSequence(t *testing.T, n *node, from, to uint64, signers []uint64) []bft.Decision {
	t.Helper()
	decisions := make([]bft.Decision, 0, to-from+1)
	for seq := from; seq <= to; seq++ {
		n.stateLock.Lock()
		prev := n.prevHash
		n.stateLock.Unlock()
		decision := buildDecision(t, seq, 0, prev,
			[]request{payment(fmt.Sprintf("terminal-%d", seq), fmt.Sprintf("%d", seq))}, signers)
		if _, err := n.applyDecision(decision.Proposal, decision.Signatures); err != nil {
			t.Fatalf("apply decision %d: %v", seq, err)
		}
		decisions = append(decisions, decision)
	}
	return decisions
}

func TestGenesisCheckpointCoversEmptyState(t *testing.T) {
	n := &node{id: 1, out: map[uint64]chan<- wireMessage{}, state: newSmallBankState(), pending: newPendingTracker(), logger: &testLogger{t: t}}
	n.initializeSyncHistory()

	header := n.syncHeader()
	if header.LatestSequence != 0 || header.LogStart != 1 || header.LogEnd != 0 {
		t.Fatalf("unexpected genesis header: %+v", header)
	}
	if len(header.Checkpoints) != 1 || header.Checkpoints[0].Sequence != 0 {
		t.Fatalf("expected one genesis checkpoint, got %+v", header.Checkpoints)
	}

	n.historyLock.Lock()
	genesis := cloneCheckpoint(n.checkpoints[0], true)
	n.historyLock.Unlock()
	if err := n.validateCheckpointState(genesis); err != nil {
		t.Fatalf("validate genesis checkpoint: %v", err)
	}
	if hashBytes(mustJSON(stateSnapshotImage(nil, nil))) !=
		hashBytes(mustJSON(stateSnapshotImage([]accountSnapshot{}, []clientSnapshot{}))) {
		t.Fatal("nil and empty slices must have the same canonical checksum")
	}
}

func TestHistoryExistsBeforeFirstPeriodicCheckpoint(t *testing.T) {
	n := checkpointTestNode(t, 1)
	deliverSequence(t, n, 1, checkpointPeriod-1, []uint64{1, 2, 3})

	header := n.syncHeader()
	if len(header.Checkpoints) != 1 || header.Checkpoints[0].Sequence != 0 {
		t.Fatalf("expected the genesis checkpoint before the first boundary, got %+v", header.Checkpoints)
	}
	if header.LogStart != 1 || header.LogEnd != checkpointPeriod-1 {
		t.Fatalf("expected retained range [1,%d], got [%d,%d]", checkpointPeriod-1, header.LogStart, header.LogEnd)
	}
	n.historyLock.Lock()
	logged := len(n.decisionLog)
	n.historyLock.Unlock()
	if logged != checkpointPeriod-1 {
		t.Fatalf("expected %d retained decisions, got %d", checkpointPeriod-1, logged)
	}
}

func TestCheckpointRetentionKeepsMultipleGenerationsAndTail(t *testing.T) {
	n := checkpointTestNode(t, 1)
	deliverSequence(t, n, 1, 4*checkpointPeriod+5, []uint64{1, 2, 3})

	header := n.syncHeader()
	var sequences []uint64
	for _, checkpoint := range header.Checkpoints {
		sequences = append(sequences, checkpoint.Sequence)
	}
	expected := []uint64{2 * checkpointPeriod, 3 * checkpointPeriod, 4 * checkpointPeriod}
	if !reflect.DeepEqual(sequences, expected) {
		t.Fatalf("expected retained checkpoints %v, got %v", expected, sequences)
	}
	if header.LogStart != 2*checkpointPeriod+1 || header.LogEnd != 4*checkpointPeriod+5 {
		t.Fatalf("unexpected retained range [%d,%d]", header.LogStart, header.LogEnd)
	}
	if err := n.validateSyncHeader(header); err != nil {
		t.Fatalf("header assembled from one history generation must validate: %v", err)
	}
}

func TestSyncHeaderRemainsConsistentWhileHistoryAdvances(t *testing.T) {
	n := checkpointTestNode(t, 1)
	done := make(chan error, 1)
	go func() {
		for seq := uint64(1); seq <= 2*checkpointPeriod+10; seq++ {
			n.stateLock.Lock()
			prev := n.prevHash
			n.stateLock.Unlock()
			decision := buildDecision(t, seq, 0, prev, nil, []uint64{1, 2, 3})
			if _, err := n.applyDecision(decision.Proposal, decision.Signatures); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("advance history: %v", err)
			}
			if err := n.validateSyncHeader(n.syncHeader()); err != nil {
				t.Fatalf("final header: %v", err)
			}
			return
		default:
			header := n.syncHeader()
			if err := n.validateSyncHeader(header); err != nil {
				t.Fatalf("observed torn header: %v", err)
			}
		}
	}
}

func TestCheckpointAgreementUsesDistinctSources(t *testing.T) {
	a := checkpointTestNode(t, 1)
	b := checkpointTestNode(t, 2)
	decisions := deliverSequence(t, a, 1, checkpointPeriod, []uint64{1, 2, 3})
	for _, decision := range decisions {
		if _, err := b.applyDecision(decision.Proposal, decision.Signatures); err != nil {
			t.Fatalf("replica b apply: %v", err)
		}
	}

	ha := a.syncHeader()
	hb := b.syncHeader()
	candidate, ok := selectCheckpointCandidate([]stateSyncSnapshot{ha, hb}, 2)
	if !ok || candidate.descriptor.Sequence != checkpointPeriod {
		t.Fatalf("expected agreement on checkpoint %d, got ok=%t candidate=%+v", checkpointPeriod, ok, candidate)
	}
	if _, ok := selectCheckpointCandidate([]stateSyncSnapshot{ha, ha}, 2); ok {
		t.Fatal("duplicate responses from one node must not satisfy f+1")
	}
}

func TestCheckpointBoundaryRequiresCertifiedDecision(t *testing.T) {
	n := checkpointTestNode(t, 1)
	decision := buildDecision(t, checkpointPeriod, 0, "", nil, []uint64{1, 2})
	descriptor := checkpointDescriptor{
		Sequence: checkpointPeriod,
		View:     0,
		Checksum: "checksum",
		Decision: decision,
	}
	if err := n.validateCheckpointDescriptor(descriptor); err == nil {
		t.Fatal("checkpoint boundary without a commit quorum must be rejected")
	}
	descriptor.Decision.Signatures = append(descriptor.Decision.Signatures, bft.Signature{ID: 3, Msg: []byte("msg"), Value: []byte("sig")})
	if err := n.validateCheckpointDescriptor(descriptor); err != nil {
		t.Fatalf("checkpoint with a member commit quorum should validate: %v", err)
	}
}

func TestSnapshotServingUsesExactImmutableObjects(t *testing.T) {
	n := checkpointTestNode(t, 1)
	deliverSequence(t, n, 1, 2*checkpointPeriod+4, []uint64{1, 2, 3})

	header, err := n.serveSyncSnapshot(grpcStateSnapshotRequest{HeaderOnly: true})
	if err != nil {
		t.Fatalf("serve header: %v", err)
	}
	if header.HasCheckpoint || len(header.Decisions) != 0 || len(header.Checkpoints) != checkpointRetention {
		t.Fatalf("header response contains unexpected payload: %+v", header)
	}
	requested := header.Checkpoints[1]
	payload, err := n.serveSyncSnapshot(grpcStateSnapshotRequest{
		WantCheckpoint:     true,
		CheckpointSequence: requested.Sequence,
		CheckpointChecksum: requested.Checksum,
		FromSequence:       checkpointPeriod + 1,
		ThroughSequence:    checkpointPeriod + 4,
	})
	if err != nil {
		t.Fatalf("serve exact payload: %v", err)
	}
	if !payload.HasCheckpoint || payload.Checkpoint.Sequence != checkpointPeriod || len(payload.Decisions) != 4 {
		t.Fatalf("unexpected exact payload: checkpoint=%d decisions=%d", payload.Checkpoint.Sequence, len(payload.Decisions))
	}
	if _, err := n.serveSyncSnapshot(grpcStateSnapshotRequest{
		WantCheckpoint:     true,
		CheckpointSequence: requested.Sequence,
		CheckpointChecksum: "wrong",
	}); err == nil {
		t.Fatal("a server must not substitute a different checkpoint")
	}
	if _, err := n.serveSyncSnapshot(grpcStateSnapshotRequest{
		FromSequence:    checkpointPeriod + 1,
		ThroughSequence: 2*checkpointPeriod + 5,
	}); err == nil {
		t.Fatal("a server must reject a range beyond its certified frontier")
	}
}

func TestSyncTargetIsFixedByFPlusOneRetainedFrontiers(t *testing.T) {
	headers := []stateSyncSnapshot{
		{NodeID: 1, LatestSequence: 500, LogStart: 101, LogEnd: 500},
		{NodeID: 2, LatestSequence: 400, LogStart: 101, LogEnd: 400},
		{NodeID: 3, LatestSequence: 300, LogStart: 101, LogEnd: 300},
		{NodeID: 4, LatestSequence: 1000, LogStart: 900, LogEnd: 1000},
	}
	target, donors, ok := selectSyncTarget(headers, 201, 2)
	if !ok || target != 400 || !reflect.DeepEqual(donors, []uint64{1, 2}) {
		t.Fatalf("expected fixed target 400 from donors [1 2], got target=%d donors=%v ok=%t", target, donors, ok)
	}
}

func TestSyncDecisionRangesAreChunked(t *testing.T) {
	tests := []struct {
		name     string
		position uint64
		target   uint64
		through  uint64
	}{
		{name: "full chunk", position: 0, target: 100, through: 32},
		{name: "next full chunk", position: 32, target: 100, through: 64},
		{name: "short final chunk", position: 96, target: 100, through: 100},
		{name: "exact final chunk", position: 68, target: 100, through: 100},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := nextSyncChunkThrough(test.position, test.target); got != test.through {
				t.Fatalf("position=%d target=%d: expected through=%d, got %d", test.position, test.target, test.through, got)
			}
			if test.through-test.position > syncDecisionChunkSize {
				t.Fatalf("chunk spans %d decisions, limit is %d", test.through-test.position, syncDecisionChunkSize)
			}
		})
	}
}

func TestSequenceGapIsRejectedBeforeStateMutation(t *testing.T) {
	n := checkpointTestNode(t, 1)
	beforeState := hashBytes(mustJSON(n.state.deterministicStateSnapshot()))
	n.stateLock.Lock()
	beforeHash := n.prevHash
	n.stateLock.Unlock()

	gap := buildDecision(t, 2, 0, beforeHash,
		[]request{payment("gap-terminal", "1")}, []uint64{1, 2, 3})
	if _, err := n.applyDecision(gap.Proposal, gap.Signatures); err == nil {
		t.Fatal("a sequence gap must be rejected")
	}
	if got := hashBytes(mustJSON(n.state.deterministicStateSnapshot())); got != beforeState {
		t.Fatal("a rejected sequence gap mutated application state")
	}
	n.stateLock.Lock()
	afterHash := n.prevHash
	n.stateLock.Unlock()
	if afterHash != beforeHash || n.currentSequence() != 0 {
		t.Fatalf("a rejected sequence gap advanced state: prev_hash=%q sequence=%d", afterHash, n.currentSequence())
	}
}

func TestDecisionPreflightFailureDoesNotMutateStateOrHistory(t *testing.T) {
	validRequest, err := encodeRequest(payment("valid-terminal", "1"))
	if err != nil {
		t.Fatalf("encode valid request: %v", err)
	}
	invalidType, err := encodeRequest(request{ClientID: "invalid-terminal", ID: "1", Type: txType("UNKNOWN")})
	if err != nil {
		t.Fatalf("encode invalid request: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*bft.Decision)
	}{
		{
			name: "malformed payload",
			mutate: func(decision *bft.Decision) {
				decision.Proposal.Payload = []byte("{")
			},
		},
		{
			name: "malformed metadata",
			mutate: func(decision *bft.Decision) {
				decision.Proposal.Metadata = []byte{0xff}
			},
		},
		{
			name: "empty metadata",
			mutate: func(decision *bft.Decision) {
				decision.Proposal.Metadata = nil
			},
		},
		{
			name: "malformed request after valid request",
			mutate: func(decision *bft.Decision) {
				decision.Proposal.Payload = encodeBlockData(blockData{Requests: [][]byte{validRequest, []byte("{")}})
			},
		},
		{
			name: "invalid request after valid request",
			mutate: func(decision *bft.Decision) {
				decision.Proposal.Payload = encodeBlockData(blockData{Requests: [][]byte{validRequest, invalidType}})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			n := checkpointTestNode(t, 1)
			n.stateLock.Lock()
			beforeState := n.state.deterministicStateSnapshot()
			beforeHash := n.prevHash
			n.stateLock.Unlock()

			decision := buildDecision(t, 1, 0, beforeHash, nil, []uint64{1, 2, 3})
			test.mutate(&decision)
			if _, err := n.applyDecision(decision.Proposal, decision.Signatures); err == nil {
				t.Fatal("invalid decision must be rejected")
			}

			n.stateLock.Lock()
			afterState := n.state.deterministicStateSnapshot()
			afterHash := n.prevHash
			n.stateLock.Unlock()
			if !reflect.DeepEqual(afterState, beforeState) {
				t.Fatal("preflight failure mutated application state")
			}
			if afterHash != beforeHash || n.currentSequence() != 0 {
				t.Fatalf("preflight failure advanced state: prev_hash=%q sequence=%d", afterHash, n.currentSequence())
			}
			n.lastLock.Lock()
			lastDelivered, lastIndex := n.lastDelivered, n.lastIndex
			n.lastLock.Unlock()
			if lastDelivered || lastIndex != 0 {
				t.Fatalf("preflight failure advanced last decision: delivered=%t sequence=%d", lastDelivered, lastIndex)
			}
		})
	}
}

func TestLiveDeliveryFailureDoesNotAdvanceLibraryCheckpoint(t *testing.T) {
	n := checkpointTestNode(t, 1)
	checkpoint := &bft.Checkpoint{}
	initial := buildDecision(t, 0, 0, "", nil, []uint64{1, 2, 3})
	checkpoint.Set(initial.Proposal, initial.Signatures)
	controller := &algorithm.Controller{
		Application: n,
		Checkpoint:  checkpoint,
		Logger:      &testLogger{t: t},
	}
	deliver := &algorithm.MutuallyExclusiveDeliver{C: controller}
	gap := buildDecision(t, 2, 0, "", []request{payment("gap-terminal", "1")}, []uint64{1, 2, 3})

	panicValue := capturePanic(func() {
		deliver.Deliver(gap.Proposal, gap.Signatures)
	})
	if panicValue == nil {
		t.Fatal("live delivery failure must stop the node")
	}

	stored, _ := checkpoint.Get()
	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(stored.Metadata, md); err != nil {
		t.Fatalf("decode stored checkpoint metadata: %v", err)
	}
	if md.GetLatestSequence() != 0 {
		t.Fatalf("library checkpoint advanced after failed delivery: sequence=%d", md.GetLatestSequence())
	}
	if n.currentSequence() != 0 {
		t.Fatalf("application history advanced after failed delivery: sequence=%d", n.currentSequence())
	}
}

func TestSuccessfulDecisionAdvancesApplicationAndHistoryTogether(t *testing.T) {
	n := checkpointTestNode(t, 1)
	decision := buildDecision(t, 1, 3, "", []request{payment("terminal-1", "1")}, []uint64{1, 2, 3})
	if _, err := n.applyDecision(decision.Proposal, decision.Signatures); err != nil {
		t.Fatalf("apply decision: %v", err)
	}

	n.stateLock.Lock()
	n.lastLock.Lock()
	n.historyLock.Lock()
	defer func() {
		n.historyLock.Unlock()
		n.lastLock.Unlock()
		n.stateLock.Unlock()
	}()

	if !n.state.isExecuted("terminal-1", "1") {
		t.Fatal("successful decision did not update application state")
	}
	if n.prevHash != decision.Proposal.Digest() {
		t.Fatalf("unexpected previous hash %q", n.prevHash)
	}
	if !n.lastDelivered || n.lastIndex != 1 || n.lastView != 3 {
		t.Fatalf("unexpected last-decision state: delivered=%t view=%d sequence=%d", n.lastDelivered, n.lastView, n.lastIndex)
	}
	if n.historyLatestSequence != n.lastIndex || n.historyLatestView != n.lastView {
		t.Fatalf("application/history mismatch: last=(%d,%d) history=(%d,%d)",
			n.lastView, n.lastIndex, n.historyLatestView, n.historyLatestSequence)
	}
	if n.historyLatestDecision.Proposal.Digest() != decision.Proposal.Digest() ||
		n.lastDecision.Proposal.Digest() != decision.Proposal.Digest() {
		t.Fatal("last decision and history do not reference the applied proposal")
	}
}

func capturePanic(fn func()) (value any) {
	defer func() {
		value = recover()
	}()
	fn()
	return nil
}

func TestMismatchedGenesisRequiresInstallation(t *testing.T) {
	local := checkpointTestNode(t, 1)
	remote := &node{id: 2, out: map[uint64]chan<- wireMessage{1: nil, 3: nil, 4: nil}, state: newSmallBankState(), pending: newPendingTracker(), logger: &testLogger{t: t}}
	remote.initializeSyncHistory()

	localHeader := local.syncHeader()
	remoteHeader := remote.syncHeader()
	remoteGenesis := remoteHeader.Checkpoints[0]
	if headerContainsCheckpoint(localHeader, remoteGenesis) {
		t.Fatal("test setup requires different deterministic genesis states")
	}
	if !checkpointNeedsInstall(localHeader, remoteGenesis) {
		t.Fatal("a mismatched agreed genesis checkpoint must be installable")
	}
	payload, err := remote.serveSyncSnapshot(grpcStateSnapshotRequest{
		WantCheckpoint:     true,
		CheckpointSequence: 0,
		CheckpointChecksum: remoteGenesis.Checksum,
	})
	if err != nil {
		t.Fatalf("serve remote genesis: %v", err)
	}
	if err := local.installStateCheckpoint(payload.Checkpoint); err != nil {
		t.Fatalf("install remote genesis: %v", err)
	}
	if !headerContainsCheckpoint(local.syncHeader(), remoteGenesis) {
		t.Fatal("installed genesis did not become authoritative")
	}
}

func TestReplayRequiresExactRangeAndCertifiedMembers(t *testing.T) {
	source := checkpointTestNode(t, 1)
	decisions := deliverSequence(t, source, 1, 5, []uint64{1, 2, 3})

	t.Run("complete range", func(t *testing.T) {
		follower := checkpointTestNode(t, 2)
		applied, err := follower.replayDecisions(decisions, 0, 5)
		if err != nil || applied != 5 {
			t.Fatalf("expected five applied decisions, got applied=%d err=%v", applied, err)
		}
		if hashBytes(mustJSON(follower.state.deterministicStateSnapshot())) !=
			hashBytes(mustJSON(source.state.deterministicStateSnapshot())) {
			t.Fatal("replay did not reproduce source state")
		}
	})

	t.Run("partial range is not success", func(t *testing.T) {
		follower := checkpointTestNode(t, 2)
		applied, err := follower.replayDecisions(decisions[:4], 0, 5)
		if err == nil || applied != 4 || follower.currentSequence() != 4 {
			t.Fatalf("expected a safe four-decision prefix and an incomplete error, applied=%d seq=%d err=%v", applied, follower.currentSequence(), err)
		}
		applied, err = follower.replayDecisions(decisions[4:], 4, 5)
		if err != nil || applied != 1 || follower.currentSequence() != 5 {
			t.Fatalf("expected donor failover to finish at five, applied=%d seq=%d err=%v", applied, follower.currentSequence(), err)
		}
	})

	t.Run("application failure preserves applied prefix", func(t *testing.T) {
		follower := checkpointTestNode(t, 2)
		corrupt := cloneDecision(decisions[1])
		corrupt.Proposal.Payload = []byte("{")
		applied, err := follower.replayDecisions([]bft.Decision{decisions[0], corrupt}, 0, 2)
		if err == nil || applied != 1 || follower.currentSequence() != 1 {
			t.Fatalf("expected one safe decision before replay failure, applied=%d seq=%d err=%v", applied, follower.currentSequence(), err)
		}
	})

	t.Run("sequence gap", func(t *testing.T) {
		follower := checkpointTestNode(t, 2)
		if _, err := follower.replayDecisions(decisions[1:], 0, 5); err == nil {
			t.Fatal("a range that skips a sequence must be rejected")
		}
	})

	t.Run("insufficient signatures", func(t *testing.T) {
		follower := checkpointTestNode(t, 2)
		under := buildDecision(t, 1, 0, "", nil, []uint64{1, 2})
		if _, err := follower.replayDecisions([]bft.Decision{under}, 0, 1); err == nil {
			t.Fatal("a decision without a commit quorum must be rejected")
		}
	})

	t.Run("duplicate and non-member signers", func(t *testing.T) {
		follower := checkpointTestNode(t, 2)
		duplicate := buildDecision(t, 1, 0, "", nil, []uint64{1, 1, 1, 1})
		if _, err := follower.replayDecisions([]bft.Decision{duplicate}, 0, 1); err == nil {
			t.Fatal("duplicate signer IDs must not satisfy the quorum")
		}
		nonMember := buildDecision(t, 1, 0, "", nil, []uint64{1, 2, 99})
		if _, err := follower.replayDecisions([]bft.Decision{nonMember}, 0, 1); err == nil {
			t.Fatal("a non-member signer must be rejected")
		}
	})

	t.Run("matches live proposal validation", func(t *testing.T) {
		follower := checkpointTestNode(t, 2)
		decision := buildDecision(t, 1, 0, "not-checked-by-live-validation", nil, []uint64{1, 2, 3})
		if _, err := follower.replayDecisions([]bft.Decision{decision}, 0, 1); err != nil {
			t.Fatalf("sync must not impose a PrevHash rule absent from VerifyProposal: %v", err)
		}
	})
}

func TestInstalledCheckpointBecomesAuthoritativeAndServable(t *testing.T) {
	source := checkpointTestNode(t, 1)
	follower := checkpointTestNode(t, 2)
	deliverSequence(t, source, 1, checkpointPeriod, []uint64{1, 2, 3})

	header := source.syncHeader()
	descriptor := header.Checkpoints[len(header.Checkpoints)-1]
	payload, err := source.serveSyncSnapshot(grpcStateSnapshotRequest{
		WantCheckpoint:     true,
		CheckpointSequence: descriptor.Sequence,
		CheckpointChecksum: descriptor.Checksum,
	})
	if err != nil {
		t.Fatalf("fetch source checkpoint: %v", err)
	}
	if err := follower.installStateCheckpoint(payload.Checkpoint); err != nil {
		t.Fatalf("install checkpoint: %v", err)
	}

	served := follower.syncHeader()
	if served.LatestSequence != checkpointPeriod || len(served.Checkpoints) != 1 ||
		!sameCheckpointDescriptor(served.Checkpoints[0], descriptor) {
		t.Fatalf("installed checkpoint did not become authoritative: %+v", served)
	}
	if err := follower.validateSyncHeader(served); err != nil {
		t.Fatalf("installed checkpoint header must validate: %v", err)
	}
}
