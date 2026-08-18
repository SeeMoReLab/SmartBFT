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
	Clients  []clientSnapshot
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
	start := time.Now()
	smallbankTracePrintf("%s event=app_sync_start node=%d local_view=%d local_seq=%d network=%t\n",
		timestampedLogTag("trace"), n.id, local.View, local.Sequence, n.network != nil)
	defer func() {
		smallbankTracePrintf("%s event=app_sync_done node=%d elapsed_ms=%d\n",
			timestampedLogTag("trace"), n.id, time.Since(start).Milliseconds())
	}()
	if n.network == nil {
		n.logStateSyncSnapshot("local", local, 1, 1)
		return bft.SyncResponse{Latest: cloneDecision(local.Latest)}
	}

	targetCount := stateSyncMatchCount(len(n.Nodes()))
	return n.fullSnapshotSync(local, targetCount)
}

func (n *node) fullSnapshotSync(local stateSyncSnapshot, targetCount int) bft.SyncResponse {
	smallbankTracePrintf("%s event=app_sync_full_snapshot_start node=%d local_view=%d local_seq=%d required=%d\n",
		timestampedLogTag("trace"), n.id, local.View, local.Sequence, targetCount)
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
				smallbankTracePrintf("%s app sync: node=%d peer=%d fetch_failed err=%v\n",
					timestampedLogTag("sync"), n.id, result.nodeID, result.err)
				continue
			}
			if err := validateStateSyncSnapshot(result.snapshot); err != nil {
				smallbankTracePrintf("%s app sync: node=%d peer=%d invalid_snapshot err=%v\n",
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
		smallbankTracePrintf("%s event=app_sync_full_snapshot_no_quorum node=%d local_view=%d local_seq=%d required=%d snapshots=%d\n",
			timestampedLogTag("trace"), n.id, local.View, local.Sequence, targetCount, len(snapshots))
		n.logStateSyncSnapshot("no-quorum", local, 1, targetCount)
		return bft.SyncResponse{Latest: cloneDecision(local.Latest)}
	}

	if compareStateSyncSnapshot(best, local) > 0 {
		if err := n.installStateSyncSnapshot(best); err != nil {
			smallbankTracePrintf("%s app sync: node=%d install_failed source=%d view=%d seq=%d err=%v\n",
				timestampedLogTag("sync"), n.id, best.NodeID, best.View, best.Sequence, err)
			return bft.SyncResponse{Latest: cloneDecision(local.Latest)}
		}
		// The installed snapshot covers batches this node never delivered, so
		// their requests were never removed from the pool by delivery.
		n.pruneExecutedFromPool()
		n.logStateSyncSnapshot("installed", best, count, targetCount)
		smallbankTracePrintf("%s event=app_sync_full_snapshot_done node=%d result=installed latest_view=%d latest_seq=%d matches=%d required=%d\n",
			timestampedLogTag("trace"), n.id, best.View, best.Sequence, count, targetCount)
		return bft.SyncResponse{Latest: cloneDecision(best.Latest)}
	}

	n.logStateSyncSnapshot("current", local, count, targetCount)
	smallbankTracePrintf("%s event=app_sync_full_snapshot_done node=%d result=current latest_view=%d latest_seq=%d matches=%d required=%d\n",
		timestampedLogTag("trace"), n.id, local.View, local.Sequence, count, targetCount)
	return bft.SyncResponse{Latest: cloneDecision(local.Latest)}
}

func (n *node) localStateSyncSnapshot() stateSyncSnapshot {
	n.stateLock.Lock()
	defer n.stateLock.Unlock()
	n.lastLock.Lock()
	defer n.lastLock.Unlock()

	image := n.state.deterministicStateSnapshot()
	return stateSyncSnapshot{
		NodeID:   n.id,
		View:     n.lastView,
		Sequence: n.lastIndex,
		Checksum: hashBytes(mustJSON(image)),
		Accounts: cloneAccountSnapshots(image.Accounts),
		Clients:  cloneClientSnapshots(image.Clients),
		Latest:   cloneDecision(n.lastDecision),
	}
}

func (n *node) installStateSyncSnapshot(snapshot stateSyncSnapshot) error {
	if err := validateStateSyncSnapshot(snapshot); err != nil {
		return err
	}

	state, err := stateFromSnapshots(snapshot.Accounts, snapshot.Clients)
	if err != nil {
		return err
	}

	n.stateLock.Lock()
	defer n.stateLock.Unlock()
	n.lastLock.Lock()
	defer n.lastLock.Unlock()

	// Carry the diagnostic count across the state replacement.
	state.duplicatesSkipped = n.state.duplicatesSkipped
	n.state = state
	if len(snapshot.Latest.Proposal.Metadata) > 0 {
		n.prevHash = snapshot.Latest.Proposal.Digest()
	} else {
		n.prevHash = ""
	}
	n.lastDelivered = len(snapshot.Latest.Proposal.Metadata) > 0
	n.lastView = snapshot.View
	n.lastIndex = snapshot.Sequence
	n.lastLeaderID = n.leaderID()
	n.lastDecision = cloneDecision(snapshot.Latest)
	return nil
}

func validateStateSyncSnapshot(snapshot stateSyncSnapshot) error {
	image := stateSnapshot{Accounts: snapshot.Accounts, Clients: snapshot.Clients}
	if snapshot.Checksum != hashBytes(mustJSON(image)) {
		return fmt.Errorf("checksum mismatch")
	}
	if _, err := stateFromSnapshots(snapshot.Accounts, snapshot.Clients); err != nil {
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
	smallbankTracePrintf("%s app sync: node=%d event=%s latest_available=%t latest_view=%d latest_seq=%d state_accounts=%d matches=%d required=%d checksum=%s source=%d\n",
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
