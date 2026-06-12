// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
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
	id            uint64
	in            <-chan wireMessage
	out           map[uint64]chan<- wireMessage
	consensus     *smartbft.Consensus
	pending       *pendingTracker
	state         *smallBankState
	stateLock     sync.Mutex
	prevHash      string
	stopChan      chan struct{}
	doneWG        sync.WaitGroup
	clock         *time.Ticker
	viewClock     *time.Ticker
	logger        smart.Logger
	configuration bft.Configuration
	failures      *proposalDelayController
}

type nodeOptions struct {
	NumNodes     int
	BatchSize    uint64
	BatchTimeout time.Duration
	Failures     *proposalDelayController
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
	}

	config := bft.DefaultConfig
	config.SelfID = id
	config.RequestBatchMaxCount = opts.BatchSize
	config.RequestBatchMaxInterval = opts.BatchTimeout
	config.RequestPoolSize = max(2*opts.BatchSize, 1024)
	config.RequestForwardTimeout = 500 * time.Millisecond
	config.RequestComplainTimeout = 5 * time.Second
	config.ViewChangeTimeout = 10 * time.Second
	config.LeaderHeartbeatTimeout = 30 * time.Second
	config.LeaderRotation = false
	config.DecisionsPerLeader = 0
	n.configuration = config

	n.consensus = &smartbft.Consensus{
		Config:             config,
		ViewChangerTicker:  n.viewClock.C,
		Scheduler:          n.clock.C,
		Logger:             logger,
		Metrics:            bftmet,
		Comm:               n,
		Signer:             n,
		MembershipNotifier: n,
		Verifier:           n,
		Application:        n,
		Assembler:          n,
		RequestInspector:   n,
		Synchronizer:       n,
		WAL:                writeAheadLog,
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
					n.consensus.HandleMessage(wm.from, msg)
				case forwardedRequest:
					n.consensus.HandleRequest(wm.from, msg.payload)
				}
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
	n.doneWG.Wait()
}

func (n *node) Nodes() []uint64 {
	nodes := make([]uint64, 0, len(n.out)+1)
	nodes = append(nodes, n.id)
	for id := range n.out {
		nodes = append(nodes, id)
	}
	return nodes
}

func (n *node) SendConsensus(targetID uint64, message *smartbftprotos.Message) {
	n.out[targetID] <- wireMessage{from: n.id, msg: proto.Clone(message)}
}

func (n *node) SendTransaction(targetID uint64, request []byte) {
	reqCopy := append([]byte(nil), request...)
	n.out[targetID] <- wireMessage{from: n.id, msg: forwardedRequest{payload: reqCopy}}
}

func (n *node) RequestID(raw []byte) bft.RequestInfo {
	req, err := decodeRequest(raw)
	if err != nil {
		return bft.RequestInfo{}
	}
	return bft.RequestInfo{ClientID: req.ClientID, ID: req.ID}
}

func (n *node) VerifyRequest(raw []byte) (bft.RequestInfo, error) {
	req, err := decodeRequest(raw)
	if err != nil {
		return bft.RequestInfo{}, err
	}
	if !validTxType(req.Type) {
		return bft.RequestInfo{}, fmt.Errorf("unknown transaction type: %s", req.Type)
	}
	return bft.RequestInfo{ClientID: req.ClientID, ID: req.ID}, nil
}

func (n *node) VerifyProposal(proposal bft.Proposal) ([]bft.RequestInfo, error) {
	data, err := decodeBlockData(proposal.Payload)
	if err != nil {
		return nil, err
	}
	requests := make([]bft.RequestInfo, 0, len(data.Requests))
	for _, raw := range data.Requests {
		info, err := n.VerifyRequest(raw)
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

func (n *node) Deliver(proposal bft.Proposal, _ []bft.Signature) bft.Reconfig {
	data, err := decodeBlockData(proposal.Payload)
	if err != nil {
		n.logger.Errorf("node %d failed to decode proposal payload: %v", n.id, err)
		return bft.Reconfig{}
	}

	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	for _, rawReq := range data.Requests {
		req, err := decodeRequest(rawReq)
		if err != nil {
			n.pending.complete(response{
				Status: statusSystemError,
				Error:  err.Error(),
			})
			continue
		}
		n.pending.complete(n.state.apply(req))
	}
	n.prevHash = proposal.Digest()

	return bft.Reconfig{InLatestDecision: false}
}

func (n *node) Sync() bft.SyncResponse {
	return bft.SyncResponse{}
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
