// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"fmt"
	"sort"
	"time"

	bft "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"google.golang.org/protobuf/proto"
)

type stateSyncSnapshot struct {
	NodeID   uint64
	View     uint64
	Sequence uint64
	Checksum string
	Accounts []accountSnapshot
	Latest   bft.Decision
}

type stateSyncVoteKey struct {
	View           uint64
	Sequence       uint64
	Checksum       string
	DecisionDigest string
}

type stateSyncCandidate struct {
	snapshot stateSyncSnapshot
	count    int
}

type stateSyncResult struct {
	nodeID   uint64
	snapshot stateSyncSnapshot
	err      error
}

func (n *node) Sync() bft.SyncResponse {
	local := n.localStateSyncSnapshot()
	if n.network == nil {
		n.logStateSyncSnapshot("local", local, 1, 1)
		return bft.SyncResponse{Latest: cloneDecision(local.Latest)}
	}

	targetCount := stateSyncMatchCount(len(n.Nodes()))
	snapshots := []stateSyncSnapshot{local}
	results := make(chan stateSyncResult, len(n.Nodes()))
	requests := 0
	for _, id := range n.Nodes() {
		if id == n.id {
			continue
		}
		requests++
		go func(peerID uint64) {
			snapshot, err := n.network.fetchStateSnapshot(peerID)
			results <- stateSyncResult{nodeID: peerID, snapshot: snapshot, err: err}
		}(id)
	}

	deadline := time.NewTimer(defaultNetworkSendTimeout + 250*time.Millisecond)
	defer deadline.Stop()
	for received := 0; received < requests; {
		select {
		case result := <-results:
			received++
			if result.err != nil {
				fmt.Printf("%s app sync: node=%d peer=%d fetch_failed err=%v\n",
					timestampedLogTag("sync"), n.id, result.nodeID, result.err)
				continue
			}
			if err := validateStateSyncSnapshot(result.snapshot); err != nil {
				fmt.Printf("%s app sync: node=%d peer=%d invalid_snapshot err=%v\n",
					timestampedLogTag("sync"), n.id, result.nodeID, err)
				continue
			}
			snapshots = append(snapshots, result.snapshot)
		case <-deadline.C:
			received = requests
		}
	}

	best, count, ok := selectStateSyncSnapshot(snapshots, targetCount)
	if !ok {
		n.logStateSyncSnapshot("no-quorum", local, 1, targetCount)
		return bft.SyncResponse{Latest: cloneDecision(local.Latest)}
	}

	if compareStateSyncSnapshot(best, local) > 0 {
		if err := n.installStateSyncSnapshot(best); err != nil {
			fmt.Printf("%s app sync: node=%d install_failed source=%d view=%d seq=%d err=%v\n",
				timestampedLogTag("sync"), n.id, best.NodeID, best.View, best.Sequence, err)
			return bft.SyncResponse{Latest: cloneDecision(local.Latest)}
		}
		n.logStateSyncSnapshot("installed", best, count, targetCount)
		return bft.SyncResponse{Latest: cloneDecision(best.Latest)}
	}

	n.logStateSyncSnapshot("current", local, count, targetCount)
	return bft.SyncResponse{Latest: cloneDecision(local.Latest)}
}

func (n *node) localStateSyncSnapshot() stateSyncSnapshot {
	n.stateLock.Lock()
	defer n.stateLock.Unlock()
	n.lastLock.Lock()
	defer n.lastLock.Unlock()

	accounts := n.state.deterministicSnapshot()
	return stateSyncSnapshot{
		NodeID:   n.id,
		View:     n.lastView,
		Sequence: n.lastIndex,
		Checksum: hashBytes(mustJSON(accounts)),
		Accounts: cloneAccountSnapshots(accounts),
		Latest:   cloneDecision(n.lastDecision),
	}
}

func (n *node) installStateSyncSnapshot(snapshot stateSyncSnapshot) error {
	if err := validateStateSyncSnapshot(snapshot); err != nil {
		return err
	}

	state, err := stateFromAccountSnapshots(snapshot.Accounts)
	if err != nil {
		return err
	}

	n.stateLock.Lock()
	defer n.stateLock.Unlock()
	n.lastLock.Lock()
	defer n.lastLock.Unlock()

	n.state = state
	if len(snapshot.Latest.Proposal.Metadata) > 0 {
		n.prevHash = snapshot.Latest.Proposal.Digest()
	} else {
		n.prevHash = ""
	}
	n.lastDelivered = len(snapshot.Latest.Proposal.Metadata) > 0
	n.lastView = snapshot.View
	n.lastIndex = snapshot.Sequence
	n.lastLeaderID = n.consensus.GetLeaderID()
	n.lastDecision = cloneDecision(snapshot.Latest)
	return nil
}

func validateStateSyncSnapshot(snapshot stateSyncSnapshot) error {
	if snapshot.Checksum != hashBytes(mustJSON(snapshot.Accounts)) {
		return fmt.Errorf("checksum mismatch")
	}
	if _, err := stateFromAccountSnapshots(snapshot.Accounts); err != nil {
		return err
	}
	view, sequence, ok, err := decisionViewSequence(snapshot.Latest)
	if err != nil {
		return err
	}
	if !ok {
		if snapshot.View != 0 || snapshot.Sequence != 0 {
			return fmt.Errorf("missing latest decision for view=%d seq=%d", snapshot.View, snapshot.Sequence)
		}
		return nil
	}
	if view != snapshot.View || sequence != snapshot.Sequence {
		return fmt.Errorf("latest decision metadata view=%d seq=%d does not match snapshot view=%d seq=%d",
			view, sequence, snapshot.View, snapshot.Sequence)
	}
	return nil
}

func selectStateSyncSnapshot(snapshots []stateSyncSnapshot, requiredCount int) (stateSyncSnapshot, int, bool) {
	candidates := make(map[stateSyncVoteKey]stateSyncCandidate)
	for _, snapshot := range snapshots {
		if err := validateStateSyncSnapshot(snapshot); err != nil {
			continue
		}
		key := stateSyncKey(snapshot)
		candidate := candidates[key]
		if candidate.count == 0 || compareStateSyncSnapshot(snapshot, candidate.snapshot) > 0 {
			candidate.snapshot = snapshot
		}
		candidate.count++
		candidates[key] = candidate
	}

	var best stateSyncCandidate
	for _, candidate := range candidates {
		if candidate.count < requiredCount {
			continue
		}
		if best.count == 0 || compareStateSyncCandidate(candidate, best) > 0 {
			best = candidate
		}
	}
	return best.snapshot, best.count, best.count > 0
}

func stateSyncKey(snapshot stateSyncSnapshot) stateSyncVoteKey {
	digest := ""
	if len(snapshot.Latest.Proposal.Metadata) > 0 {
		digest = snapshot.Latest.Proposal.Digest()
	}
	return stateSyncVoteKey{
		View:           snapshot.View,
		Sequence:       snapshot.Sequence,
		Checksum:       snapshot.Checksum,
		DecisionDigest: digest,
	}
}

func compareStateSyncSnapshot(a stateSyncSnapshot, b stateSyncSnapshot) int {
	if a.Sequence != b.Sequence {
		if a.Sequence > b.Sequence {
			return 1
		}
		return -1
	}
	if a.View != b.View {
		if a.View > b.View {
			return 1
		}
		return -1
	}
	return 0
}

func compareStateSyncCandidate(a stateSyncCandidate, b stateSyncCandidate) int {
	if cmp := compareStateSyncSnapshot(a.snapshot, b.snapshot); cmp != 0 {
		return cmp
	}
	if a.count != b.count {
		if a.count > b.count {
			return 1
		}
		return -1
	}
	aKey := stateSyncKey(a.snapshot)
	bKey := stateSyncKey(b.snapshot)
	if aKey.Checksum != bKey.Checksum {
		if aKey.Checksum > bKey.Checksum {
			return 1
		}
		return -1
	}
	if aKey.DecisionDigest != bKey.DecisionDigest {
		if aKey.DecisionDigest > bKey.DecisionDigest {
			return 1
		}
		return -1
	}
	if a.snapshot.NodeID != b.snapshot.NodeID {
		if a.snapshot.NodeID > b.snapshot.NodeID {
			return 1
		}
		return -1
	}
	return 0
}

func stateSyncMatchCount(nodeCount int) int {
	if nodeCount <= 1 {
		return 1
	}
	f := (nodeCount - 1) / 3
	return f + 1
}

func (n *node) logStateSyncSnapshot(event string, snapshot stateSyncSnapshot, matches int, required int) {
	available := len(snapshot.Latest.Proposal.Metadata) > 0
	fmt.Printf("%s app sync: node=%d event=%s latest_available=%t latest_view=%d latest_seq=%d state_accounts=%d matches=%d required=%d checksum=%s source=%d\n",
		timestampedLogTag("sync"), n.id, event, available, snapshot.View, snapshot.Sequence,
		len(snapshot.Accounts), matches, required, snapshot.Checksum, snapshot.NodeID)
}

func decisionViewSequence(decision bft.Decision) (uint64, uint64, bool, error) {
	if len(decision.Proposal.Metadata) == 0 {
		return 0, 0, false, nil
	}
	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(decision.Proposal.Metadata, md); err != nil {
		return 0, 0, false, fmt.Errorf("unmarshal latest decision metadata: %w", err)
	}
	return md.GetViewId(), md.GetLatestSequence(), true, nil
}

func stateFromAccountSnapshots(accounts []accountSnapshot) (*smallBankState, error) {
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

func cloneDecision(decision bft.Decision) bft.Decision {
	return bft.Decision{
		Proposal:   cloneProposal(decision.Proposal),
		Signatures: cloneSignatures(decision.Signatures),
	}
}
