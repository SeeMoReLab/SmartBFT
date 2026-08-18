// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	smart "github.com/hyperledger-labs/SmartBFT/pkg/api"
	smartbft "github.com/hyperledger-labs/SmartBFT/pkg/consensus"
	bft "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/pkg/wal"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"google.golang.org/protobuf/proto"
)

type wireMessage struct {
	from uint64
	msg  any
}

type forwardedRequest struct {
	payload []byte
}

type node struct {
	id             uint64
	in             <-chan wireMessage
	out            map[uint64]chan<- wireMessage
	consensus      *smartbft.Consensus
	pending        *pendingTracker
	state          *smallBankState
	stateLock      sync.Mutex
	prevHash       string
	lastLock       sync.Mutex
	viewObserved   bool
	currentView    uint64
	nextView       uint64
	proposalSeq    uint64
	lastDelivered  bool
	lastView       uint64
	lastIndex      uint64
	lastLeaderID   uint64
	lastDecision   bft.Decision
	shutdownLogged bool
	stopChan       chan struct{}
	doneWG         sync.WaitGroup
	clock          *time.Ticker
	viewClock      *time.Ticker
	logger         smart.Logger
	configuration  bft.Configuration
	failures       *proposalDelayController
	learning       *learningManager
	network        *networkTransport
	replies        *clientReplyDispatcher
	timeoutBackoff *requestTimeoutBackoff

	// State transfer history is authoritative for sync discovery. It keeps a
	// small number of immutable checkpoint generations and the certified
	// decisions following the oldest retained checkpoint. The latest fields are
	// updated under the same lock, so a sync header cannot combine a new frontier
	// with an older log (or the reverse).
	historyLock           sync.Mutex
	checkpoints           []stateCheckpoint
	decisionLog           []loggedDecision
	historyLatestView     uint64
	historyLatestSequence uint64
	historyLatestDecision bft.Decision
}

type nodeOptions struct {
	NumNodes        int
	BatchSize       uint64
	BatchTimeout    time.Duration
	Failures        *proposalDelayController
	LearningOptions learningOptions
	Learning        *learningManager
	Backoff         requestTimeoutBackoffOptions
	Network         *networkTransport
	Replies         *clientReplyDispatcher
}

func newNode(
	id uint64,
	in <-chan wireMessage,
	out map[uint64]chan<- wireMessage,
	pending *pendingTracker,
	logger smart.Logger,
	walmet *wal.Metrics,
	bftmet *smart.Metrics,
	opts nodeOptions,
	testDir string,
) (*node, error) {
	nodeDir := filepath.Join(testDir, fmt.Sprintf("smallbank-node-%d", id))
	writeAheadLog, err := wal.Create(logger, nodeDir, &wal.Options{Metrics: walmet.With("node", fmt.Sprintf("%d", id))})
	if err != nil {
		return nil, fmt.Errorf("create WAL for node %d: %w", id, err)
	}

	n := &node{
		id:        id,
		in:        in,
		out:       out,
		pending:   pending,
		state:     newSmallBankState(),
		stopChan:  make(chan struct{}),
		clock:     time.NewTicker(10 * time.Millisecond),
		viewClock: time.NewTicker(100 * time.Millisecond),
		logger:    logger,
		failures:  opts.Failures,
		learning:  opts.Learning,
		network:   opts.Network,
		replies:   opts.Replies,
	}
	// The genesis checkpoint must capture the complete deterministic initial
	// application state. SmallBank starts empty and creates accounts through
	// transactions, so initialization belongs here before consensus can deliver.
	n.initializeSyncHistory()

	config := bft.DefaultConfig
	config.SelfID = id
	config.RequestBatchMaxCount = opts.BatchSize
	config.RequestBatchMaxInterval = opts.BatchTimeout
	config.RequestPoolSize = max(2*opts.BatchSize, 1024)
	config.RequestPoolSubmitTimeout = 500 * time.Millisecond
	config.RequestForwardTimeout = 400 * time.Millisecond
	config.RequestComplainTimeout = 400 * time.Millisecond
	config.ViewChangeTimeout = config.RequestForwardTimeout + config.RequestComplainTimeout
	config.ViewChangeResendInterval = config.ViewChangeTimeout
	config.LeaderHeartbeatTimeout = 30 * time.Second
	config.LeaderRotation = false
	config.DecisionsPerLeader = 0
	n.configuration = config
	timeoutBackoff, err := newRequestTimeoutBackoff(config.RequestForwardTimeout+config.RequestComplainTimeout, opts.Backoff)
	if err != nil {
		writeAheadLog.Close()
		return nil, fmt.Errorf("create request timeout backoff for node %d: %w", id, err)
	}
	n.timeoutBackoff = timeoutBackoff

	n.consensus = &smartbft.Consensus{
		Config:                    config,
		ViewChangerTicker:         n.viewClock.C,
		Scheduler:                 n.clock.C,
		Logger:                    logger,
		Metrics:                   bftmet,
		Comm:                      n,
		Signer:                    n,
		MembershipNotifier:        n,
		Verifier:                  n,
		Application:               n,
		Assembler:                 n,
		RequestInspector:          n,
		Synchronizer:              n,
		WAL:                       writeAheadLog,
		RequestTimeout:            n.onRequestTimeoutBackoff,
		ViewEvent:                 n.onViewEvent,
		ExternalViewChangeBackoff: opts.Backoff.Enabled,
		Metadata: &smartbftprotos.ViewMetadata{
			LatestSequence: 0,
			ViewId:         0,
		},
	}

	if err := n.consensus.Start(); err != nil {
		n.clock.Stop()
		n.viewClock.Stop()
		return nil, fmt.Errorf("start consensus for node %d: %w", id, err)
	}
	if n.learning != nil && n.learning.enabled {
		learningPrintf("SmartBFT request timeout active: node=%d source=initial-configuration total_timeout_ms=%d forward_timeout_ms=%d complain_timeout_ms=%d view_change_timeout_ms=%d view_change_resend_ms=%d\n",
			n.id, (config.RequestForwardTimeout + config.RequestComplainTimeout).Milliseconds(),
			config.RequestForwardTimeout.Milliseconds(), config.RequestComplainTimeout.Milliseconds(),
			config.ViewChangeTimeout.Milliseconds(), config.ViewChangeResendInterval.Milliseconds())
	}
	n.start()

	return n, nil
}

func (n *node) start() {
	n.doneWG.Add(1)
	go func() {
		defer n.doneWG.Done()
		for {
			select {
			case <-n.stopChan:
				return
			case wm := <-n.in:
				switch msg := wm.msg.(type) {
				case *smartbftprotos.Message:
					start := time.Now()
					smallbankTracePrintf("%s event=local_consensus_start node=%d from=%d %s\n",
						timestampedLogTag("trace"), n.id, wm.from, smartBFTTraceMessageSummary(msg))
					n.consensus.HandleMessage(wm.from, msg)
					smallbankTracePrintf("%s event=local_consensus_done node=%d from=%d elapsed_ms=%d %s\n",
						timestampedLogTag("trace"), n.id, wm.from, time.Since(start).Milliseconds(), smartBFTTraceMessageSummary(msg))
				case forwardedRequest:
					_ = n.handleRequest(wm.from, msg.payload)
				}
			}
		}
	}()
	n.startFailureObserver()
}

func (n *node) startFailureObserver() {
	if n.failures == nil || !n.failures.enabled {
		return
	}

	n.doneWG.Add(1)
	go func() {
		defer n.doneWG.Done()

		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		observe := func() {
			leaderID := n.consensus.GetLeaderID()
			if leaderID == 0 {
				return
			}
			n.failures.observeLeader(leaderID, n.Nodes())
		}

		observe()
		for {
			select {
			case <-n.stopChan:
				return
			case <-ticker.C:
				observe()
			}
		}
	}()
}

func (n *node) stop() {
	select {
	case <-n.stopChan:
	default:
		close(n.stopChan)
	}
	n.clock.Stop()
	n.viewClock.Stop()
	n.consensus.Stop()
	n.learning.close()
	n.replies.close()
	n.doneWG.Wait()
	n.printShutdownState()
}

func (n *node) recordObservedView(currentView uint64, nextView uint64, proposalSeq uint64) {
	n.lastLock.Lock()
	defer n.lastLock.Unlock()

	n.viewObserved = true
	n.currentView = currentView
	n.nextView = nextView
	n.proposalSeq = proposalSeq
}

func (n *node) printShutdownState() {
	// Read the execution state before taking lastLock: the rest of the file
	// acquires stateLock first, and reversing that order here would deadlock.
	n.stateLock.Lock()
	duplicatesSkipped := n.state.duplicatesSkipped
	clientsTracked := len(n.state.lastExecuted)
	n.stateLock.Unlock()

	n.lastLock.Lock()
	defer n.lastLock.Unlock()

	if n.shutdownLogged {
		return
	}
	n.shutdownLogged = true

	if !n.lastDelivered {
		fmt.Printf("%s SmartBFT SmallBank shutdown: node=%d current_view_known=%t current_view=%d next_view=%d proposal_seq=%d last_committed=false\n",
			timestampedLogTag("shutdown"), n.id, n.viewObserved, n.currentView, n.nextView, n.proposalSeq)
		n.printDedupState(duplicatesSkipped, clientsTracked)
		return
	}
	fmt.Printf("%s SmartBFT SmallBank shutdown: node=%d current_view_known=%t current_view=%d next_view=%d proposal_seq=%d last_committed_view=%d last_committed_seq=%d last_committed_leader=%d\n",
		timestampedLogTag("shutdown"), n.id, n.viewObserved, n.currentView, n.nextView, n.proposalSeq,
		n.lastView, n.lastIndex, n.lastLeaderID)
	n.printDedupState(duplicatesSkipped, clientsTracked)
}

func (n *node) printDedupState(duplicatesSkipped uint64, clientsTracked int) {
	fmt.Printf("%s request dedup: node=%d duplicates_skipped=%d clients_tracked=%d\n",
		timestampedLogTag("dedup"), n.id, duplicatesSkipped, clientsTracked)
}

func (n *node) Nodes() []uint64 {
	if n.network != nil {
		return n.network.nodeIDs()
	}
	nodes := make([]uint64, 0, len(n.out)+1)
	nodes = append(nodes, n.id)
	for id := range n.out {
		nodes = append(nodes, id)
	}
	return nodes
}

func (n *node) SendConsensus(targetID uint64, message *smartbftprotos.Message) {
	if n.network != nil {
		n.network.sendConsensus(targetID, proto.Clone(message).(*smartbftprotos.Message))
		return
	}
	out := n.out[targetID]
	clone := proto.Clone(message)
	out <- wireMessage{from: n.id, msg: clone}
}

func (n *node) SendTransaction(targetID uint64, request []byte) {
	reqCopy := append([]byte(nil), request...)
	if n.network != nil {
		n.network.sendTransaction(targetID, reqCopy)
		return
	}
	out := n.out[targetID]
	out <- wireMessage{from: n.id, msg: forwardedRequest{payload: reqCopy}}
}

func (n *node) RequestID(raw []byte) bft.RequestInfo {
	req, err := decodeRequest(raw)
	if err != nil {
		return bft.RequestInfo{}
	}
	return bft.RequestInfo{ClientID: req.ClientID, ID: req.ID}
}

// errRequestAlreadyExecuted rejects a request whose client sequence number the
// application has already applied.
var errRequestAlreadyExecuted = errors.New("request was already executed")

// isExecuted reports whether the client sequence number has already been
// applied. Used to keep replays out of the request pool.
func (n *node) isExecuted(clientID, requestID string) bool {
	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	return n.state.isExecuted(clientID, requestID)
}

// submitRequest admits a client request into consensus unless this node has
// already executed it. This is the client-facing half of the same filter
// VerifyRequest applies to requests forwarded by peers.
func (n *node) submitRequest(clientID, requestID string, raw []byte) error {
	info, err := verifyRequestPayload(raw)
	if err != nil {
		return err
	}
	if info.ClientID != clientID || info.ID != requestID {
		return fmt.Errorf("request identity mismatch: payload=%s:%s argument=%s:%s", info.ClientID, info.ID, clientID, requestID)
	}
	if n.isExecuted(clientID, requestID) {
		n.completeExecutedRequest(info)
		return nil
	}
	if err := n.consensus.SubmitRequest(raw); err != nil {
		return err
	}

	// Delivery or state transfer can advance the watermark after the first
	// check but before request-pool insertion. Recheck after insertion so either
	// this path removes the stale request or a later state-transfer prune does.
	if n.removeFromPoolIfExecuted(info) {
		n.completeExecutedRequest(info)
	}
	return nil
}

// handleRequest applies the same post-insertion watermark check to requests
// forwarded by another replica. Controller.HandleRequest validates before it
// inserts, but state transfer can advance the watermark between those steps.
func (n *node) handleRequest(sender uint64, raw []byte) error {
	info, err := verifyRequestPayload(raw)
	if err != nil {
		return err
	}
	if n.removeFromPoolIfExecuted(info) {
		n.completeExecutedRequest(info)
		return nil
	}
	if err := n.consensus.HandleRequest(sender, raw); err != nil {
		return err
	}
	if n.removeFromPoolIfExecuted(info) {
		n.completeExecutedRequest(info)
	}
	return nil
}

// completeExecutedRequest resolves a local or remote retry without executing
// it again. The most recent request for a client reuses its original response;
// older requests receive a deterministic duplicate response.
func (n *node) completeExecutedRequest(info bft.RequestInfo) {
	n.stateLock.Lock()
	if !n.state.isExecuted(info.ClientID, info.ID) {
		n.stateLock.Unlock()
		return
	}
	resp, hit := n.state.lastResponse[info.ClientID]
	if !hit || resp.ID != info.ID {
		resp = response{
			ClientID: info.ClientID,
			ID:       info.ID,
			Status:   statusDuplicate,
		}
	}
	n.stateLock.Unlock()

	if n.pending != nil {
		n.pending.complete(resp)
	}
	n.replies.reply(resp)
}

// removeFromPoolIfExecuted removes info when the application watermark
// advanced concurrently with admission. Returning true means the request is
// covered by replicated application state regardless of whether another path
// already removed it from the pool.
func (n *node) removeFromPoolIfExecuted(info bft.RequestInfo) bool {
	if !n.isExecuted(info.ClientID, info.ID) {
		return false
	}
	if n.consensus != nil && n.consensus.Pool != nil {
		_ = n.consensus.Pool.RemoveRequest(info)
	}
	return true
}

// pruneExecutedFromPool drops requests the node has already executed from the
// request pool. A state transfer installs a checkpoint without delivering the
// batches it skipped, so requests belonging to those batches are never removed
// by delivery and would hold pool slots until they were proposed again.
func (n *node) pruneExecutedFromPool() {
	if n.consensus == nil || n.consensus.Pool == nil {
		return
	}

	n.consensus.Pool.Prune(func(raw []byte) error {
		info, err := verifyRequestPayload(raw)
		if err != nil {
			return err
		}
		if n.isExecuted(info.ClientID, info.ID) {
			return errRequestAlreadyExecuted
		}
		return nil
	})
}

// verifyRequestPayload checks that a request is well formed. It deliberately
// does not consider execution history: it is the check applied to requests
// inside a proposal, where a duplicate must not invalidate the whole batch and
// depose the leader. Duplicates in a proposal are skipped at execution instead.
func verifyRequestPayload(raw []byte) (bft.RequestInfo, error) {
	req, err := decodeValidRequest(raw)
	if err != nil {
		return bft.RequestInfo{}, err
	}
	return bft.RequestInfo{ClientID: req.ClientID, ID: req.ID}, nil
}

// decodeValidRequest performs every request check needed before execution.
// applyDecision uses it during preflight so no malformed request can cause a
// partial batch application.
func decodeValidRequest(raw []byte) (request, error) {
	req, err := decodeRequest(raw)
	if err != nil {
		return request{}, err
	}
	if !validTxType(req.Type) {
		return request{}, fmt.Errorf("unknown transaction type: %s", req.Type)
	}
	if _, err := parseClientSeq(req.ID); err != nil {
		return request{}, err
	}
	return req, nil
}

// VerifyRequest is the admission check the consensus layer runs before a
// request enters the request pool, which is where requests forwarded by a peer
// arrive. Rejecting requests this node has already executed keeps replays -
// copies that arrive after the original was committed, and entries a peer's
// pool kept across a state transfer - from occupying pool slots until they are
// proposed and committed a second time.
func (n *node) VerifyRequest(raw []byte) (bft.RequestInfo, error) {
	info, err := verifyRequestPayload(raw)
	if err != nil {
		return bft.RequestInfo{}, err
	}
	if n.isExecuted(info.ClientID, info.ID) {
		return bft.RequestInfo{}, fmt.Errorf("%w: %s:%s", errRequestAlreadyExecuted, info.ClientID, info.ID)
	}
	return info, nil
}

func (n *node) VerifyProposal(proposal bft.Proposal) ([]bft.RequestInfo, error) {
	data, err := decodeBlockData(proposal.Payload)
	if err != nil {
		return nil, err
	}
	requests := make([]bft.RequestInfo, 0, len(data.Requests))
	for _, raw := range data.Requests {
		info, err := verifyRequestPayload(raw)
		if err != nil {
			return nil, err
		}
		requests = append(requests, info)
	}
	return requests, nil
}

func (n *node) RequestsFromProposal(proposal bft.Proposal) []bft.RequestInfo {
	data, err := decodeBlockData(proposal.Payload)
	if err != nil {
		return nil
	}
	requests := make([]bft.RequestInfo, 0, len(data.Requests))
	for _, raw := range data.Requests {
		info := n.RequestID(raw)
		if info.ClientID != "" || info.ID != "" {
			requests = append(requests, info)
		}
	}
	return requests
}

func (n *node) AssembleProposal(metadata []byte, requests [][]byte) bft.Proposal {
	if delay := n.failures.delayForProposal(n.id, n.consensus.GetLeaderID(), n.Nodes()); delay > 0 {
		fmt.Printf("Applying proposal delay: node=%d replica=%d delay_ms=%d\n",
			n.id, smartNodeIDToFailureReplicaID(n.id), delay.Milliseconds())
		time.Sleep(delay)
	}

	payload := encodeBlockData(blockData{Requests: requests})
	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(metadata, md); err != nil {
		panic(fmt.Sprintf("unmarshal proposal metadata: %v", err))
	}

	n.stateLock.Lock()
	prevHash := n.prevHash
	n.stateLock.Unlock()

	return bft.Proposal{
		Header: encodeBlockHeader(blockHeader{
			PrevHash: prevHash,
			DataHash: hashBytes(payload),
			Sequence: int64(md.LatestSequence),
		}),
		Payload:  payload,
		Metadata: metadata,
	}
}

// decisionApplication reports what applying one committed decision did, so that
// the caller can attribute it to live consensus or to state-transfer replay.
type decisionApplication struct {
	metadata  *smartbftprotos.ViewMetadata
	batchSize int
	latencies []time.Duration
	elapsed   time.Duration
}

// applyDecision applies one committed decision to the application state. It is
// the shared core of Deliver and of state-transfer replay, so that a replayed
// decision produces exactly the same state transition as a delivered one.
func (n *node) applyDecision(proposal bft.Proposal, signatures []bft.Signature) (decisionApplication, error) {
	data, err := decodeBlockData(proposal.Payload)
	if err != nil {
		return decisionApplication{}, fmt.Errorf("decode proposal payload: %w", err)
	}

	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(proposal.Metadata, md); err != nil {
		return decisionApplication{}, fmt.Errorf("decode proposal metadata: %w", err)
	}
	if len(proposal.Metadata) == 0 {
		return decisionApplication{}, fmt.Errorf("proposal metadata is empty")
	}

	decisionTime := time.Now()
	latencies := make([]time.Duration, 0, len(data.Requests))
	decoded := make([]request, 0, len(data.Requests))
	for i, rawReq := range data.Requests {
		req, err := decodeValidRequest(rawReq)
		if err != nil {
			return decisionApplication{}, fmt.Errorf("decode request %d: %w", i, err)
		}
		decoded = append(decoded, req)
		if latency, exists := n.pending.latencyFor(req, decisionTime); exists {
			latencies = append(latencies, latency)
		}
	}
	decision := bft.Decision{
		Proposal:   cloneProposal(proposal),
		Signatures: cloneSignatures(signatures),
	}

	postDecisionStart := time.Now()
	responses, err := n.commitDecision(md, decision, decoded)
	if err != nil {
		return decisionApplication{}, err
	}
	for _, resp := range responses {
		n.pending.complete(resp)
		n.replies.reply(resp)
	}

	return decisionApplication{
		metadata:  md,
		batchSize: len(data.Requests),
		latencies: latencies,
		elapsed:   time.Since(postDecisionStart),
	}, nil
}

// commitDecision changes application state, last-decision state, and sync
// history as one lock-protected operation. All data-dependent validation must
// finish before this function is called. After the sequence check succeeds,
// the remaining operations are deterministic and have no ordinary error path.
func (n *node) commitDecision(md *smartbftprotos.ViewMetadata, decision bft.Decision, requests []request) ([]response, error) {
	leaderID := n.leaderID()
	n.stateLock.Lock()
	defer n.stateLock.Unlock()
	n.lastLock.Lock()
	defer n.lastLock.Unlock()
	n.historyLock.Lock()
	defer n.historyLock.Unlock()

	sequence := md.GetLatestSequence()
	expected := n.historyLatestSequence + 1
	if sequence != expected {
		return nil, fmt.Errorf("state history sequence gap: expected %d, got %d", expected, sequence)
	}

	responses := make([]response, 0, len(requests))
	for _, req := range requests {
		responses = append(responses, n.state.apply(req))
	}
	n.prevHash = decision.Proposal.Digest()
	n.lastDelivered = true
	n.lastView = md.GetViewId()
	n.lastIndex = sequence
	n.lastLeaderID = leaderID
	n.lastDecision = decision

	var image *stateSnapshot
	if isCheckpointSequence(sequence) {
		captured := n.state.deterministicStateSnapshot()
		image = &captured
	}
	n.appendHistoryLocked(md, decision, image)
	return responses, nil
}

func (n *node) Deliver(proposal bft.Proposal, signatures []bft.Signature) bft.Reconfig {
	applied, err := n.applyDecision(proposal, signatures)
	if err != nil {
		panic(fmt.Sprintf("node %d cannot apply committed decision: %v", n.id, err))
	}
	md := applied.metadata

	n.learning.recordConsensus(learningSample{
		Sequence:     md.GetLatestSequence(),
		View:         md.GetViewId(),
		LeaderID:     n.consensus.GetLeaderID(),
		BatchSize:    applied.batchSize,
		DecisionTime: time.Now(),
		Latencies:    applied.latencies,
		Timeout:      n.learning.currentTimeoutValue(),
	})
	n.onCommitBackoff(md.GetViewId(), md.GetLatestSequence())
	if md.GetLatestSequence() == 1 || md.GetLatestSequence()%500 == 0 || applied.elapsed > 100*time.Millisecond {
		fmt.Printf("%s delivered: node=%d view=%d seq=%d batch=%d post_decision_ms=%d state_accounts=%d\n",
			timestampedLogTag("sync"), n.id, md.GetViewId(), md.GetLatestSequence(), applied.batchSize,
			applied.elapsed.Milliseconds(), n.stateAccountCount())
	}

	return bft.Reconfig{InLatestDecision: false}
}

func (n *node) applyBaseRequestTimeout(timeout time.Duration, source string) (bft.Configuration, error) {
	if n.timeoutBackoff != nil && n.timeoutBackoff.state().Enabled {
		update, err := n.timeoutBackoff.setBaseTimeout(timeout)
		if err != nil {
			return n.configuration, err
		}
		return n.applyEffectiveTimeouts(update.State, source)
	}

	// Keep the view-change timers tied to the learned request timeout, mirroring
	// applyEffectiveTimeouts. ApplyViewChangeTimeout must run first: it clamps the
	// resend interval down to the new timeout, and the explicit resend call after it
	// raises the interval again when the timeout grows.
	config, err := n.consensus.ApplyViewChangeTimeout(timeout)
	if err != nil {
		return config, err
	}
	if err := n.consensus.ApplyViewChangeResendInterval(timeout); err != nil {
		return config, err
	}
	config, err = n.consensus.ApplyRequestTimeout(timeout)
	if err != nil {
		return config, err
	}
	n.configuration = config
	learningPrintf("applied SmartBFT request timeout: node=%d source=%s total_timeout_ms=%d forward_timeout_ms=%d complain_timeout_ms=%d view_change_timeout_ms=%d view_change_resend_ms=%d\n",
		n.id, source, timeout.Milliseconds(), config.RequestForwardTimeout.Milliseconds(), config.RequestComplainTimeout.Milliseconds(),
		config.ViewChangeTimeout.Milliseconds(), timeout.Milliseconds())
	return config, nil
}

func (n *node) onRequestTimeoutBackoff(view uint64) {
	update := n.timeoutBackoff.onRequestTimeout(view)
	if !update.Log {
		return
	}
	fmt.Printf("%s request timeout: node=%d view=%d base_timeout_ms=%d multiplier=%d effective_timeout_ms=%d max_timeout_ms=%d applied=%t\n",
		timestampedLogTag("backoff"), n.id, view, update.State.BaseTimeout.Milliseconds(), update.State.Multiplier,
		update.State.EffectiveTimeout.Milliseconds(), update.State.MaxTimeout.Milliseconds(), update.Apply)
}

func (n *node) onNoProgressViewChangeBackoff(targetView uint64) {
	update := n.timeoutBackoff.onNoProgressViewChange(targetView)
	if !update.Log {
		return
	}
	n.learning.recordNoProgressViewChange()
	if update.Apply {
		if _, err := n.applyEffectiveTimeouts(update.State, "backoff-view-change"); err != nil {
			n.logger.Errorf("node %d failed to apply timeout backoff after view change to %d: %v", n.id, targetView, err)
			return
		}
	}
	fmt.Printf("%s no-progress view change: node=%d target_view=%d base_timeout_ms=%d multiplier=%d effective_timeout_ms=%d effective_view_change_timeout_ms=%d effective_view_change_resend_ms=%d max_timeout_ms=%d applied=%t\n",
		timestampedLogTag("backoff"), n.id, targetView, update.State.BaseTimeout.Milliseconds(), update.State.Multiplier,
		update.State.EffectiveTimeout.Milliseconds(), update.State.EffectiveViewChangeTimeout.Milliseconds(),
		update.State.EffectiveViewChangeResendInterval.Milliseconds(), update.State.MaxTimeout.Milliseconds(), update.Apply)
}

func (n *node) onCommitBackoff(view uint64, sequence uint64) {
	update := n.timeoutBackoff.onCommit(view, sequence)
	if !update.Log {
		return
	}
	if update.Apply {
		if _, err := n.applyEffectiveTimeouts(update.State, "backoff-commit"); err != nil {
			n.logger.Errorf("node %d failed to apply request timeout backoff after commit view=%d seq=%d: %v", n.id, view, sequence, err)
			return
		}
	}
	if update.Decayed {
		fmt.Printf("%s decay: node=%d view=%d seq=%d base_timeout_ms=%d previous_multiplier=%d multiplier=%d previous_effective_timeout_ms=%d effective_timeout_ms=%d previous_effective_view_change_timeout_ms=%d effective_view_change_timeout_ms=%d previous_effective_view_change_resend_ms=%d effective_view_change_resend_ms=%d max_timeout_ms=%d applied=%t\n",
			timestampedLogTag("backoff"), n.id, view, sequence, update.State.BaseTimeout.Milliseconds(),
			update.Previous.Multiplier, update.State.Multiplier,
			update.Previous.EffectiveTimeout.Milliseconds(), update.State.EffectiveTimeout.Milliseconds(),
			update.Previous.EffectiveViewChangeTimeout.Milliseconds(), update.State.EffectiveViewChangeTimeout.Milliseconds(),
			update.Previous.EffectiveViewChangeResendInterval.Milliseconds(), update.State.EffectiveViewChangeResendInterval.Milliseconds(),
			update.State.MaxTimeout.Milliseconds(), update.Apply)
	}
	fmt.Printf("%s committed: node=%d view=%d seq=%d base_timeout_ms=%d multiplier=%d effective_timeout_ms=%d effective_view_change_timeout_ms=%d effective_view_change_resend_ms=%d max_timeout_ms=%d applied=%t\n",
		timestampedLogTag("backoff"), n.id, view, sequence, update.State.BaseTimeout.Milliseconds(), update.State.Multiplier,
		update.State.EffectiveTimeout.Milliseconds(), update.State.EffectiveViewChangeTimeout.Milliseconds(),
		update.State.EffectiveViewChangeResendInterval.Milliseconds(), update.State.MaxTimeout.Milliseconds(), update.Apply)
}

func (n *node) applyEffectiveTimeouts(state requestTimeoutBackoffState, source string) (bft.Configuration, error) {
	config, err := n.consensus.ApplyViewChangeTimeout(state.BaseTimeout)
	if err != nil {
		return config, err
	}
	if err := n.consensus.ApplyViewChangeBackoffFactor(uint64(state.Multiplier)); err != nil {
		return config, err
	}
	if err := n.consensus.ApplyViewChangeResendInterval(state.EffectiveViewChangeResendInterval); err != nil {
		return config, err
	}
	config, err = n.consensus.ApplyRequestTimeout(state.EffectiveTimeout)
	if err != nil {
		return config, err
	}
	n.configuration = config
	fmt.Printf("%s applied SmartBFT timeouts: node=%d source=%s base_timeout_ms=%d multiplier=%d effective_timeout_ms=%d effective_view_change_timeout_ms=%d effective_view_change_resend_ms=%d max_timeout_ms=%d forward_timeout_ms=%d complain_timeout_ms=%d view_change_timeout_ms=%d view_change_backoff_factor=%d\n",
		timestampedLogTag("backoff"), n.id, source, state.BaseTimeout.Milliseconds(), state.Multiplier,
		state.EffectiveTimeout.Milliseconds(), state.EffectiveViewChangeTimeout.Milliseconds(),
		state.EffectiveViewChangeResendInterval.Milliseconds(), state.MaxTimeout.Milliseconds(),
		config.RequestForwardTimeout.Milliseconds(), config.RequestComplainTimeout.Milliseconds(),
		config.ViewChangeTimeout.Milliseconds(), state.Multiplier)
	return config, nil
}

func (n *node) onViewEvent(event string, nodeID uint64, currentView uint64, nextView uint64, proposalSeq uint64, backoffFactor uint64, detail string) {
	n.recordObservedView(currentView, nextView, proposalSeq)
	if event == "start_view_change" {
		n.learning.recordViewChange()
		n.onNoProgressViewChangeBackoff(nextView)
	}
}

func timestampedLogTag(component string) string {
	return fmt.Sprintf("[%s %s]", component, time.Now().Format("2006-01-02T15:04:05.000Z07:00"))
}

func smallbankTracePrintf(format string, args ...any) {
	// fmt.Printf(format, args...)
}

func (n *node) stateAccountCount() int {
	n.stateLock.Lock()
	defer n.stateLock.Unlock()
	return len(n.state.accounts)
}

func (n *node) leaderID() uint64 {
	if n.consensus == nil {
		return 0
	}
	return n.consensus.GetLeaderID()
}

func cloneProposal(proposal bft.Proposal) bft.Proposal {
	return bft.Proposal{
		Payload:              append([]byte(nil), proposal.Payload...),
		Header:               append([]byte(nil), proposal.Header...),
		Metadata:             append([]byte(nil), proposal.Metadata...),
		VerificationSequence: proposal.VerificationSequence,
	}
}

func cloneSignatures(signatures []bft.Signature) []bft.Signature {
	if len(signatures) == 0 {
		return nil
	}
	cloned := make([]bft.Signature, 0, len(signatures))
	for _, signature := range signatures {
		cloned = append(cloned, bft.Signature{
			ID:    signature.ID,
			Value: append([]byte(nil), signature.Value...),
			Msg:   append([]byte(nil), signature.Msg...),
		})
	}
	return cloned
}

func (n *node) MembershipChange() bool {
	return false
}

func (n *node) Sign([]byte) []byte {
	return nil
}

func (n *node) SignProposal(_ bft.Proposal, auxiliaryInput []byte) *bft.Signature {
	return &bft.Signature{ID: n.id, Msg: auxiliaryInput}
}

func (n *node) VerifyConsenterSig(signature bft.Signature, _ bft.Proposal) ([]byte, error) {
	return signature.Msg, nil
}

func (n *node) VerifySignature(_ bft.Signature) error {
	return nil
}

func (n *node) VerificationSequence() uint64 {
	return 0
}

func (n *node) AuxiliaryData(msg []byte) []byte {
	return msg
}

func validTxType(t txType) bool {
	switch t {
	case txDepositChecking, txTransactSavings, txWriteCheck, txSendPayment, txAmalgamate, txBalance, txCreateAccount:
		return true
	default:
		return false
	}
}
