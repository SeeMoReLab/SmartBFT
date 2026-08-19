// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger-labs/SmartBFT/examples/internal/fabrictransport"
	smart "github.com/hyperledger-labs/SmartBFT/pkg/api"
	"github.com/hyperledger-labs/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger-labs/SmartBFT/pkg/wal"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	defaultNetworkSendTimeout = 2 * time.Second
	syncMaxReceiveMessageSize = 64 << 20
	consensusQueueSize        = 100
	clientQueueSize           = 65536
	operationConsensus        = "smallbank-consensus"
	operationTransaction      = "smallbank-transaction"
	operationSubmit           = "smallbank-submit"
	operationStatus           = "smallbank-status"
	operationChecksum         = "smallbank-checksum"
	operationStateTransfer    = "smallbank-state-transfer"
	operationApplyTimeout     = "smallbank-apply-timeout"
	operationClientReply      = "smallbank-client-reply"
)

type hostEntry struct {
	ID   uint64
	Host string
	Port int
}

func (h hostEntry) address() string {
	return net.JoinHostPort(h.Host, strconv.Itoa(h.Port))
}

func loadNetworkHosts(path string) ([]hostEntry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var hosts []hostEntry
	for lineNo, rawLine := range strings.Split(string(raw), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 3 {
			return nil, fmt.Errorf("%s:%d: expected at least 3 columns: node_id host port", path, lineNo+1)
		}
		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("%s:%d: invalid SmartBFT node id %q", path, lineNo+1, parts[0])
		}
		port, err := strconv.Atoi(parts[2])
		if err != nil || port <= 0 || port > 65535 {
			return nil, fmt.Errorf("%s:%d: invalid port %q", path, lineNo+1, parts[2])
		}
		hosts = append(hosts, hostEntry{ID: id, Host: parts[1], Port: port})
	}
	if len(hosts) == 0 {
		return nil, fmt.Errorf("no SmartBFT hosts found in %s", path)
	}
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].ID < hosts[j].ID })
	for i := 1; i < len(hosts); i++ {
		if hosts[i-1].ID == hosts[i].ID {
			return nil, fmt.Errorf("duplicate SmartBFT node id %d in %s", hosts[i].ID, path)
		}
	}
	return hosts, nil
}

func hostByID(hosts []hostEntry, id uint64) (hostEntry, bool) {
	for _, host := range hosts {
		if host.ID == id {
			return host, true
		}
	}
	return hostEntry{}, false
}

type grpcAck struct{}

type grpcConsensusRequest struct {
	From    uint64
	Message []byte
}

type grpcTransactionRequest struct {
	From    uint64
	Payload []byte
}

type grpcSubmitRequest struct {
	Payload      []byte
	Mode         string
	ReplyAddress string
}

type grpcStatusRequest struct{}

type grpcStatusResponse struct {
	NodeID  uint64
	Leader  uint64
	Running bool
}

type grpcChecksumRequest struct{}

type grpcChecksumResponse struct {
	NodeID   uint64
	Checksum string
}

// grpcStateSnapshotRequest selects an immutable checkpoint or an exact,
// inclusive decision range. HeaderOnly returns only the peer's certified
// frontier and retained checkpoint descriptors.
type grpcStateSnapshotRequest struct {
	HeaderOnly         bool
	WantCheckpoint     bool
	CheckpointSequence uint64
	CheckpointChecksum string
	FromSequence       uint64
	ThroughSequence    uint64
}

type grpcApplyTimeoutRequest struct {
	TimeoutMS int64
}

type grpcReplyRequest struct {
	From     uint64
	Response response
}

type networkTransport struct {
	selfID    uint64
	hosts     map[uint64]hostEntry
	clients   map[uint64]*fabrictransport.Client
	closeOnce sync.Once
}

func smallBankRequestTrace(raw []byte) string {
	req, err := decodeRequest(raw)
	if err != nil {
		return fmt.Sprintf("request_decode_error=%q request_bytes=%d", err.Error(), len(raw))
	}
	return fmt.Sprintf("request={%s %s}", req.ClientID, req.ID)
}

func smartBFTTraceMessageSummary(message *smartbftprotos.Message) string {
	if message == nil {
		return "type=nil view=na seq=na"
	}
	switch msg := message.GetContent().(type) {
	case *smartbftprotos.Message_PrePrepare:
		pp := msg.PrePrepare
		if pp == nil {
			return "type=pre_prepare view=na seq=na"
		}
		batchBytes := 0
		if pp.Proposal != nil {
			batchBytes = len(pp.Proposal.Payload)
		}
		return fmt.Sprintf("type=pre_prepare view=%d seq=%d proposal_bytes=%d", pp.GetView(), pp.GetSeq(), batchBytes)
	case *smartbftprotos.Message_Prepare:
		prepare := msg.Prepare
		if prepare == nil {
			return "type=prepare view=na seq=na"
		}
		return fmt.Sprintf("type=prepare view=%d seq=%d", prepare.GetView(), prepare.GetSeq())
	case *smartbftprotos.Message_Commit:
		commit := msg.Commit
		if commit == nil {
			return "type=commit view=na seq=na"
		}
		signer := uint64(0)
		if commit.GetSignature() != nil {
			signer = commit.GetSignature().GetSigner()
		}
		return fmt.Sprintf("type=commit view=%d seq=%d signer=%d", commit.GetView(), commit.GetSeq(), signer)
	case *smartbftprotos.Message_ViewChange:
		vc := msg.ViewChange
		if vc == nil {
			return "type=view_change next_view=na"
		}
		return fmt.Sprintf("type=view_change next_view=%d", vc.GetNextView())
	case *smartbftprotos.Message_ViewData:
		nextView := uint64(0)
		signer := uint64(0)
		if msg.ViewData != nil {
			signer = msg.ViewData.GetSigner()
			vd := &smartbftprotos.ViewData{}
			if err := proto.Unmarshal(msg.ViewData.GetRawViewData(), vd); err == nil {
				nextView = vd.GetNextView()
			}
		}
		return fmt.Sprintf("type=view_data next_view=%d signer=%d", nextView, signer)
	case *smartbftprotos.Message_NewView:
		count := 0
		nextView := uint64(0)
		if msg.NewView != nil {
			count = len(msg.NewView.GetSignedViewData())
			for _, svd := range msg.NewView.GetSignedViewData() {
				vd := &smartbftprotos.ViewData{}
				if err := proto.Unmarshal(svd.GetRawViewData(), vd); err == nil {
					nextView = vd.GetNextView()
					break
				}
			}
		}
		return fmt.Sprintf("type=new_view next_view=%d view_data_count=%d", nextView, count)
	case *smartbftprotos.Message_HeartBeat:
		hb := msg.HeartBeat
		if hb == nil {
			return "type=heartbeat view=na seq=na"
		}
		return fmt.Sprintf("type=heartbeat view=%d seq=%d", hb.GetView(), hb.GetSeq())
	case *smartbftprotos.Message_HeartBeatResponse:
		hbr := msg.HeartBeatResponse
		if hbr == nil {
			return "type=heartbeat_response view=na"
		}
		return fmt.Sprintf("type=heartbeat_response view=%d", hbr.GetView())
	case *smartbftprotos.Message_StateTransferRequest:
		return "type=state_transfer_request"
	case *smartbftprotos.Message_StateTransferResponse:
		st := msg.StateTransferResponse
		if st == nil {
			return "type=state_transfer_response view=na seq=na"
		}
		return fmt.Sprintf("type=state_transfer_response view=%d seq=%d", st.GetViewNum(), st.GetSequence())
	default:
		return fmt.Sprintf("type=%T", msg)
	}
}

func newNetworkTransport(selfID uint64, hosts []hostEntry) (*networkTransport, error) {
	t := &networkTransport{
		selfID:  selfID,
		hosts:   make(map[uint64]hostEntry, len(hosts)),
		clients: make(map[uint64]*fabrictransport.Client),
	}
	for _, host := range hosts {
		t.hosts[host.ID] = host
		if host.ID == selfID {
			continue
		}
		peerID := host.ID
		client, err := newSmallBankFabricClient(selfID, host.address(), consensusQueueSize, func(operation string, err error) {
			smallbankTracePrintf("%s event=stream_failed node=%d to=%d operation=%s err=%q\n",
				timestampedLogTag("trace"), selfID, peerID, operation, err.Error())
		})
		if err != nil {
			t.close()
			return nil, fmt.Errorf("create transport to node %d at %s: %w", host.ID, host.address(), err)
		}
		t.clients[host.ID] = client
	}
	return t, nil
}

func newSmallBankFabricClient(
	selfID uint64,
	target string,
	queueSize int,
	onError func(operation string, err error),
) (*fabrictransport.Client, error) {
	return fabrictransport.NewClient(fabrictransport.ClientConfig{
		SelfID:            selfID,
		Address:           target,
		QueueSize:         queueSize,
		SendTimeout:       defaultNetworkSendTimeout,
		MaxReceiveMsgSize: syncMaxReceiveMessageSize,
		OnError:           onError,
	})
}

func (t *networkTransport) close() {
	if t == nil {
		return
	}
	t.closeOnce.Do(func() {
		for id, client := range t.clients {
			if err := client.Close(); err != nil {
				smallbankTracePrintf("%s event=transport_close_failed node=%d to=%d err=%q\n",
					timestampedLogTag("trace"), t.selfID, id, err.Error())
			}
		}
	})
}

func (t *networkTransport) nodeIDs() []uint64 {
	ids := make([]uint64, 0, len(t.hosts))
	for id := range t.hosts {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (t *networkTransport) sendConsensus(targetID uint64, message *smartbftprotos.Message) {
	client := t.clients[targetID]
	if client == nil {
		smallbankTracePrintf("%s event=send_consensus_drop node=%d to=%d reason=no_route %s\n",
			timestampedLogTag("trace"), t.selfID, targetID, smartBFTTraceMessageSummary(message))
		return
	}
	messageRaw, err := proto.Marshal(message)
	if err != nil {
		smallbankTracePrintf("%s event=send_consensus_drop node=%d to=%d reason=marshal_message err=%q\n",
			timestampedLogTag("trace"), t.selfID, targetID, err.Error())
		return
	}
	payload, err := fabrictransport.Marshal(&grpcConsensusRequest{Message: messageRaw})
	if err != nil {
		smallbankTracePrintf("%s event=send_consensus_drop node=%d to=%d reason=marshal_envelope err=%q\n",
			timestampedLogTag("trace"), t.selfID, targetID, err.Error())
		return
	}
	trace := smartBFTTraceMessageSummary(message)
	smallbankTracePrintf("%s event=send_consensus_enqueue node=%d to=%d %s\n",
		timestampedLogTag("trace"), t.selfID, targetID, trace)
	err = client.Send(operationConsensus, payload, true, func(sendErr error) {
		if sendErr != nil {
			smallbankTracePrintf("%s event=send_consensus_failed node=%d to=%d err=%q %s\n",
				timestampedLogTag("trace"), t.selfID, targetID, sendErr.Error(), trace)
			return
		}
		smallbankTracePrintf("%s event=send_consensus_sent node=%d to=%d %s\n",
			timestampedLogTag("trace"), t.selfID, targetID, trace)
	})
	if err != nil {
		smallbankTracePrintf("%s event=send_consensus_drop node=%d to=%d reason=enqueue err=%q %s\n",
			timestampedLogTag("trace"), t.selfID, targetID, err.Error(), trace)
	}
}

func (t *networkTransport) sendTransaction(targetID uint64, raw []byte) {
	client := t.clients[targetID]
	trace := smallBankRequestTrace(raw)
	if client == nil {
		smallbankTracePrintf("%s event=forward_failed node=%d to=%d reason=no_route %s\n",
			timestampedLogTag("trace"), t.selfID, targetID, trace)
		return
	}
	payload, err := fabrictransport.Marshal(&grpcTransactionRequest{Payload: append([]byte(nil), raw...)})
	if err != nil {
		smallbankTracePrintf("%s event=forward_failed node=%d to=%d reason=marshal err=%q %s\n",
			timestampedLogTag("trace"), t.selfID, targetID, err.Error(), trace)
		return
	}
	smallbankTracePrintf("%s event=forward_enqueue node=%d to=%d %s\n",
		timestampedLogTag("trace"), t.selfID, targetID, trace)
	err = client.Send(operationTransaction, payload, false, func(sendErr error) {
		if sendErr != nil {
			smallbankTracePrintf("%s event=forward_send_failed node=%d to=%d err=%q %s\n",
				timestampedLogTag("trace"), t.selfID, targetID, sendErr.Error(), trace)
			return
		}
		smallbankTracePrintf("%s event=forward_sent node=%d to=%d %s\n",
			timestampedLogTag("trace"), t.selfID, targetID, trace)
	})
	if err != nil {
		smallbankTracePrintf("%s event=forward_failed node=%d to=%d reason=enqueue err=%q %s\n",
			timestampedLogTag("trace"), t.selfID, targetID, err.Error(), trace)
	}
}

func (t *networkTransport) fetchStateSnapshot(ctx context.Context, targetID uint64, req grpcStateSnapshotRequest, timeout time.Duration) (stateSyncSnapshot, error) {
	client := t.clients[targetID]
	if client == nil {
		return stateSyncSnapshot{}, fmt.Errorf("no route to target node %d", targetID)
	}
	if ctx == nil {
		return stateSyncSnapshot{}, fmt.Errorf("state snapshot request to node %d has no context", targetID)
	}
	if timeout <= 0 {
		return stateSyncSnapshot{}, fmt.Errorf("state snapshot request to node %d has invalid timeout %s", targetID, timeout)
	}
	request, err := fabrictransport.Marshal(&req)
	if err != nil {
		return stateSyncSnapshot{}, fmt.Errorf("encode state snapshot request to node %d: %w", targetID, err)
	}
	rpcCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	smallbankTracePrintf("%s event=sync_stream_start node=%d to=%d operation=%s\n",
		timestampedLogTag("trace"), t.selfID, targetID, operationStateTransfer)
	raw, err := client.Call(rpcCtx, operationStateTransfer, request)
	if err != nil {
		smallbankTracePrintf("%s event=sync_stream_done node=%d to=%d operation=%s elapsed_ms=%d err=%q\n",
			timestampedLogTag("trace"), t.selfID, targetID, operationStateTransfer, time.Since(start).Milliseconds(), err.Error())
		return stateSyncSnapshot{}, err
	}
	var snapshot stateSyncSnapshot
	if err := fabrictransport.Unmarshal(raw, &snapshot); err != nil {
		return stateSyncSnapshot{}, fmt.Errorf("decode state snapshot from node %d: %w", targetID, err)
	}
	// Bind the application-level identity to the peer selected by the transport.
	if snapshot.NodeID != targetID {
		return stateSyncSnapshot{}, fmt.Errorf("target node %d returned identity %d", targetID, snapshot.NodeID)
	}
	smallbankTracePrintf("%s event=sync_stream_done node=%d to=%d operation=%s elapsed_ms=%d err=\"\" view=%d seq=%d source=%d\n",
		timestampedLogTag("trace"), t.selfID, targetID, operationStateTransfer, time.Since(start).Milliseconds(), snapshot.LatestView, snapshot.LatestSequence, snapshot.NodeID)
	return snapshot, nil
}

type replyTracker struct {
	lock      sync.Mutex
	quorum    int
	waiters   map[string]*replyWaiter
	completed map[string]response
}

type replyWaiter struct {
	ch       chan response
	matches  map[response]int
	received map[uint64]struct{}
}

func newReplyTracker(quorum int) *replyTracker {
	return &replyTracker{
		quorum:    quorum,
		waiters:   make(map[string]*replyWaiter),
		completed: make(map[string]response),
	}
}

func (t *replyTracker) register(req request) (<-chan response, func()) {
	key := requestKey(req.ClientID, req.ID)
	ch := make(chan response, 1)

	t.lock.Lock()
	if resp, exists := t.completed[key]; exists {
		ch <- resp
		close(ch)
		t.lock.Unlock()
		return ch, func() {}
	}
	t.waiters[key] = &replyWaiter{
		ch:       ch,
		matches:  make(map[response]int),
		received: make(map[uint64]struct{}),
	}
	t.lock.Unlock()

	cancel := func() {
		t.lock.Lock()
		delete(t.waiters, key)
		t.lock.Unlock()
	}

	return ch, cancel
}

func (t *replyTracker) observe(from uint64, resp response) {
	key := requestKey(resp.ClientID, resp.ID)

	t.lock.Lock()
	waiter, exists := t.waiters[key]
	if !exists {
		t.lock.Unlock()
		return
	}
	if _, duplicate := waiter.received[from]; duplicate {
		t.lock.Unlock()
		return
	}
	waiter.received[from] = struct{}{}
	waiter.matches[resp]++
	if waiter.matches[resp] < t.quorum {
		t.lock.Unlock()
		return
	}
	delete(t.waiters, key)
	t.completed[key] = resp
	t.lock.Unlock()

	waiter.ch <- resp
	close(waiter.ch)
}

type clientReplyServer struct {
	tracker *replyTracker
}

func (s *clientReplyServer) Handle(_ context.Context, from uint64, operation string, payload []byte) ([]byte, error) {
	if operation != operationClientReply {
		return nil, fmt.Errorf("unknown SmallBank client operation %q", operation)
	}
	if from == 0 {
		return nil, errors.New("client reply is missing its sender ID")
	}
	request := new(grpcReplyRequest)
	if err := fabrictransport.Unmarshal(payload, request); err != nil {
		return nil, fmt.Errorf("decode client reply: %w", err)
	}
	request.From = from
	s.tracker.observe(request.From, request.Response)
	return nil, nil
}

type clientReplyDispatcher struct {
	selfID  uint64
	lock    sync.Mutex
	targets map[string]string
	senders map[string]*replySender
}

type replySender struct {
	address string
	client  *fabrictransport.Client
}

type submitSender struct {
	host         hostEntry
	client       *fabrictransport.Client
	replyAddress string
}

func newClientReplyDispatcher(selfID uint64) *clientReplyDispatcher {
	return &clientReplyDispatcher{
		selfID:  selfID,
		targets: make(map[string]string),
		senders: make(map[string]*replySender),
	}
}

func (d *clientReplyDispatcher) remember(req request, address string) {
	if d == nil || address == "" {
		return
	}
	key := requestKey(req.ClientID, req.ID)

	d.lock.Lock()
	d.targets[key] = address
	d.lock.Unlock()
}

func (d *clientReplyDispatcher) reply(resp response) {
	if d == nil {
		return
	}
	key := requestKey(resp.ClientID, resp.ID)

	d.lock.Lock()
	address, exists := d.targets[key]
	if exists {
		delete(d.targets, key)
	}
	sender := d.senders[address]
	if exists && sender == nil {
		var err error
		sender, err = newReplySender(address, d.selfID)
		if err != nil {
			fmt.Printf("SmallBank reply transport to %s failed: %v\n", address, err)
		} else {
			d.senders[address] = sender
		}
	}
	d.lock.Unlock()

	if !exists || sender == nil {
		return
	}
	sender.enqueue(resp)
}

func (d *clientReplyDispatcher) close() {
	if d == nil {
		return
	}
	d.lock.Lock()
	senders := make([]*replySender, 0, len(d.senders))
	for _, sender := range d.senders {
		senders = append(senders, sender)
	}
	d.lock.Unlock()

	for _, sender := range senders {
		sender.close()
	}
}

func newReplySender(address string, selfID uint64) (*replySender, error) {
	client, err := newSmallBankFabricClient(selfID, address, clientQueueSize, func(operation string, err error) {
		fmt.Printf("SmallBank reply stream to %s failed: operation=%s err=%v\n", address, operation, err)
	})
	if err != nil {
		return nil, err
	}
	return &replySender{address: address, client: client}, nil
}

func (s *replySender) enqueue(resp response) {
	payload, err := fabrictransport.Marshal(&grpcReplyRequest{Response: resp})
	if err != nil {
		fmt.Printf("SmallBank reply encoding for %s failed: %v\n", s.address, err)
		return
	}
	// A disconnected benchmark client must not block replica delivery. This is
	// equivalent to Fabric ending that client's Deliver stream.
	if err := s.client.Send(operationClientReply, payload, true, nil); err != nil {
		fmt.Printf("SmallBank reply enqueue for %s failed: %v\n", s.address, err)
	}
}

func (s *replySender) close() {
	if err := s.client.Close(); err != nil {
		fmt.Printf("SmallBank reply transport close for %s failed: %v\n", s.address, err)
	}
}

func newSubmitSender(host hostEntry, client *fabrictransport.Client, replyAddress string) *submitSender {
	return &submitSender{host: host, client: client, replyAddress: replyAddress}
}

func (s *submitSender) enqueue(_ request, raw []byte) {
	payload, err := fabrictransport.Marshal(&grpcSubmitRequest{
		Payload:      append([]byte(nil), raw...),
		Mode:         string(submitModeBroadcast),
		ReplyAddress: s.replyAddress,
	})
	if err != nil {
		fmt.Printf("SmallBank submit encoding for node %d failed: %v\n", s.host.ID, err)
		return
	}
	if err := s.client.Send(operationSubmit, payload, false, nil); err != nil {
		fmt.Printf("SmallBank submit enqueue for node %d failed: %v\n", s.host.ID, err)
	}
}

func (s *submitSender) close() {}

type networkNodeServer struct {
	node           *node
	host           hostEntry
	grpcServer     *grpc.Server
	listener       net.Listener
	requestTimeout time.Duration
	closeOnce      sync.Once
}

func newNetworkNodeServer(
	id uint64,
	hosts []hostEntry,
	opts nodeOptions,
	dataDir string,
	logMode smallBankLogMode,
	requestTimeout time.Duration,
) (*networkNodeServer, error) {
	host, exists := hostByID(hosts, id)
	if !exists {
		return nil, fmt.Errorf("node id %d not found in hosts config", id)
	}

	transport, err := newNetworkTransport(id, hosts)
	if err != nil {
		return nil, err
	}
	logger := newSmallBankLogger(logMode)
	met := &disabled.Provider{}
	walMet := wal.NewMetrics(met, "smallbank")
	bftMet := smart.NewMetrics(met, "smallbank")

	in := make(chan wireMessage)
	pending := newPendingTracker()
	nodeOpts := opts
	nodeOpts.NumNodes = len(hosts)
	nodeOpts.Network = transport
	nodeOpts.Replies = newClientReplyDispatcher(id)
	var localNode *node
	learningOpts := opts.LearningOptions
	if learningOpts.Enabled {
		if learningOpts.NodeID == 0 {
			learningOpts.NodeID = 1
		}
		if learningOpts.NodeID == id {
			learningOpts.ApplyTimeout = func(timeout time.Duration) error {
				if localNode == nil {
					return fmt.Errorf("node %d not initialized", id)
				}
				_, err := localNode.applyBaseRequestTimeout(timeout, "learning-recommendation")
				return err
			}
		} else {
			learningOpts = learningOptions{}
		}
	}
	learning, err := newLearningManager(learningOpts)
	if err != nil {
		transport.close()
		return nil, err
	}
	nodeOpts.Learning = learning
	n, err := newNode(id, in, nil, pending, logger, walMet, bftMet, nodeOpts, dataDir)
	if err != nil {
		learning.close()
		transport.close()
		return nil, err
	}
	localNode = n

	listener, err := net.Listen("tcp", host.address())
	if err != nil {
		n.stop()
		transport.close()
		return nil, err
	}
	s := &networkNodeServer{
		node:           n,
		host:           host,
		listener:       listener,
		grpcServer:     grpc.NewServer(),
		requestTimeout: requestTimeout,
	}
	if err := fabrictransport.RegisterServer(s.grpcServer, s, fabrictransport.ServerConfig{
		SendTimeout: defaultNetworkSendTimeout,
		OnError: func(operation string, err error) {
			smallbankTracePrintf("%s event=server_stream_failed node=%d operation=%s err=%q\n",
				timestampedLogTag("trace"), id, operation, err.Error())
		},
	}); err != nil {
		_ = listener.Close()
		n.stop()
		transport.close()
		return nil, err
	}
	return s, nil
}

func (s *networkNodeServer) serve() error {
	fmt.Printf("SmartBFT SmallBank gRPC server listening: node=%d address=%s\n", s.host.ID, s.host.address())
	err := s.grpcServer.Serve(s.listener)
	if err != nil && err != grpc.ErrServerStopped {
		return err
	}
	return nil
}

func (s *networkNodeServer) close(ctx context.Context) {
	s.closeOnce.Do(func() {
		s.closeOnceClose(ctx)
	})
}

func (s *networkNodeServer) closeOnceClose(ctx context.Context) {
	s.node.printShutdownState()
	done := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.grpcServer.Stop()
	}
	s.node.network.close()
	s.node.stop()
}

// Handle dispatches all node, client, control, and state-transfer traffic from
// the shared Fabric-style Step service.
func (s *networkNodeServer) Handle(ctx context.Context, from uint64, operation string, payload []byte) ([]byte, error) {
	switch operation {
	case operationConsensus:
		request := new(grpcConsensusRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode consensus request: %w", err)
		}
		request.From = from
		return nil, s.handleConsensusRequest(request)
	case operationTransaction:
		request := new(grpcTransactionRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode forwarded transaction: %w", err)
		}
		request.From = from
		_, err := s.Transaction(ctx, request)
		return nil, err
	case operationSubmit:
		request := new(grpcSubmitRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode client submission: %w", err)
		}
		mode, err := parseSubmitMode(request.Mode)
		if err != nil {
			return nil, err
		}
		if mode == submitModeBroadcast {
			decoded, err := decodeRequest(request.Payload)
			if err != nil {
				return nil, fmt.Errorf("decode broadcast submission: %w", err)
			}
			if err := s.acceptBroadcastSubmit(decoded, request); err != nil {
				return nil, err
			}
			return fabrictransport.Marshal(&response{ClientID: decoded.ClientID, ID: decoded.ID, Status: statusSuccess})
		}
		result, err := s.Submit(ctx, request)
		if err != nil {
			return nil, err
		}
		return fabrictransport.Marshal(result)
	case operationStatus:
		request := new(grpcStatusRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode status request: %w", err)
		}
		result, err := s.Status(ctx, request)
		if err != nil {
			return nil, err
		}
		return fabrictransport.Marshal(result)
	case operationChecksum:
		request := new(grpcChecksumRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode checksum request: %w", err)
		}
		result, err := s.Checksum(ctx, request)
		if err != nil {
			return nil, err
		}
		return fabrictransport.Marshal(result)
	case operationStateTransfer:
		request := new(grpcStateSnapshotRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode state-transfer request: %w", err)
		}
		result, err := s.StateSnapshot(ctx, request)
		if err != nil {
			return nil, err
		}
		return fabrictransport.Marshal(result)
	case operationApplyTimeout:
		request := new(grpcApplyTimeoutRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode timeout request: %w", err)
		}
		_, err := s.ApplyTimeout(ctx, request)
		return nil, err
	default:
		return nil, fmt.Errorf("unknown SmallBank transport operation %q", operation)
	}
}

func (s *networkNodeServer) handleConsensusRequest(req *grpcConsensusRequest) error {
	msg := &smartbftprotos.Message{}
	if err := proto.Unmarshal(req.Message, msg); err != nil {
		return grpcstatus.Errorf(codes.InvalidArgument, "decode consensus message: %v", err)
	}
	start := time.Now()
	smallbankTracePrintf("%s event=recv_consensus_start node=%d from=%d %s\n",
		timestampedLogTag("trace"), s.node.id, req.From, smartBFTTraceMessageSummary(msg))
	defer func() {
		smallbankTracePrintf("%s event=recv_consensus_done node=%d from=%d elapsed_ms=%d %s\n",
			timestampedLogTag("trace"), s.node.id, req.From, time.Since(start).Milliseconds(), smartBFTTraceMessageSummary(msg))
	}()
	s.node.consensus.HandleMessage(req.From, msg)
	return nil
}

func (s *networkNodeServer) Transaction(_ context.Context, req *grpcTransactionRequest) (*grpcAck, error) {
	start := time.Now()
	trace := smallBankRequestTrace(req.Payload)
	smallbankTracePrintf("%s event=recv_forward_start node=%d from=%d %s\n",
		timestampedLogTag("trace"), s.node.id, req.From, trace)
	defer func() {
		smallbankTracePrintf("%s event=recv_forward_done node=%d from=%d elapsed_ms=%d %s\n",
			timestampedLogTag("trace"), s.node.id, req.From, time.Since(start).Milliseconds(), trace)
	}()
	if err := s.node.handleRequest(req.From, req.Payload); err != nil {
		smallbankTracePrintf("%s event=recv_forward_submit_result node=%d from=%d result=rejected err=%q %s\n",
			timestampedLogTag("trace"), s.node.id, req.From, err.Error(), trace)
		return &grpcAck{}, nil
	}
	smallbankTracePrintf("%s event=recv_forward_submit_result node=%d from=%d result=accepted err=\"\" %s\n",
		timestampedLogTag("trace"), s.node.id, req.From, trace)
	return &grpcAck{}, nil
}

func (s *networkNodeServer) Submit(ctx context.Context, req *grpcSubmitRequest) (*response, error) {
	decoded, err := decodeRequest(req.Payload)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "decode request: %v", err)
	}

	mode, err := parseSubmitMode(req.Mode)
	if err != nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, err.Error())
	}
	leaderID := s.node.consensus.GetLeaderID()
	if leaderID == 0 {
		return nil, grpcstatus.Error(codes.Unavailable, "leader unavailable")
	}
	if mode == submitModeLeader && leaderID != s.node.id {
		return nil, grpcstatus.Errorf(codes.FailedPrecondition, "not leader: leader=%d", leaderID)
	}

	if mode == submitModeBroadcast {
		if err := s.acceptBroadcastSubmit(decoded, req); err != nil {
			return nil, grpcstatus.Errorf(codes.Unavailable, "submit request: %v", err)
		}
		return &response{ClientID: decoded.ClientID, ID: decoded.ID, Status: statusSuccess}, nil
	}

	respCh, cancel := s.node.pending.register(decoded)
	defer cancel()

	if err := s.node.submitRequest(decoded.ClientID, decoded.ID, req.Payload); err != nil {
		s.node.pending.fail(decoded, err)
		return nil, grpcstatus.Errorf(codes.Unavailable, "submit request: %v", err)
	}

	waitCtx, cancelTimeout := context.WithTimeout(ctx, s.requestTimeout)
	defer cancelTimeout()
	select {
	case resp := <-respCh:
		return &resp, nil
	case <-waitCtx.Done():
		return nil, grpcstatus.Error(codes.DeadlineExceeded, waitCtx.Err().Error())
	}
}

func (s *networkNodeServer) acceptBroadcastSubmit(decoded request, req *grpcSubmitRequest) error {
	if req.ReplyAddress == "" {
		return fmt.Errorf("broadcast submit missing reply address")
	}
	s.node.replies.remember(decoded, req.ReplyAddress)
	s.node.pending.markSubmitted(decoded)
	return s.node.submitRequest(decoded.ClientID, decoded.ID, req.Payload)
}

func (s *networkNodeServer) Status(context.Context, *grpcStatusRequest) (*grpcStatusResponse, error) {
	return &grpcStatusResponse{
		NodeID:  s.node.id,
		Leader:  s.node.consensus.GetLeaderID(),
		Running: s.node.consensus.GetLeaderID() != 0,
	}, nil
}

func (s *networkNodeServer) Checksum(context.Context, *grpcChecksumRequest) (*grpcChecksumResponse, error) {
	s.node.stateLock.Lock()
	raw := mustJSON(s.node.state.deterministicStateSnapshot())
	s.node.stateLock.Unlock()
	return &grpcChecksumResponse{NodeID: s.node.id, Checksum: hashBytes(raw)}, nil
}

func (s *networkNodeServer) StateSnapshot(_ context.Context, req *grpcStateSnapshotRequest) (*stateSyncSnapshot, error) {
	start := time.Now()
	smallbankTracePrintf("%s event=recv_sync_start node=%d method=StateSnapshot header_only=%t want_checkpoint=%t checkpoint=%d from=%d through=%d\n",
		timestampedLogTag("trace"), s.node.id, req.HeaderOnly, req.WantCheckpoint,
		req.CheckpointSequence, req.FromSequence, req.ThroughSequence)
	snapshot, err := s.node.serveSyncSnapshot(*req)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.FailedPrecondition, "serve state snapshot: %v", err)
	}
	smallbankTracePrintf("%s event=recv_sync_done node=%d method=StateSnapshot elapsed_ms=%d checkpoint_seq=%d latest_seq=%d decisions=%d\n",
		timestampedLogTag("trace"), s.node.id, time.Since(start).Milliseconds(), snapshot.Checkpoint.Sequence, snapshot.LatestSequence, len(snapshot.Decisions))
	return &snapshot, nil
}

func (s *networkNodeServer) ApplyTimeout(_ context.Context, req *grpcApplyTimeoutRequest) (*grpcAck, error) {
	if req.TimeoutMS <= 0 {
		return nil, grpcstatus.Error(codes.InvalidArgument, "timeout_ms must be positive")
	}
	_, err := s.node.applyBaseRequestTimeout(time.Duration(req.TimeoutMS)*time.Millisecond, "learning-recommendation")
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "apply timeout: %v", err)
	}
	return &grpcAck{}, nil
}

type networkSmallBankClient struct {
	hosts          []hostEntry
	clients        map[uint64]*fabrictransport.Client
	next           atomic.Uint64
	leader         atomic.Uint64
	requestTimeout time.Duration
	submitMode     submitMode
	replyListen    string
	replyAdvertise string
	replyTracker   *replyTracker
	replyAddress   string
	replyServer    *grpc.Server
	replyListener  net.Listener
	submitSenders  map[uint64]*submitSender
}

type submitMode string

const (
	submitModeBroadcast submitMode = "broadcast"
	submitModeLeader    submitMode = "leader"
)

func parseSubmitMode(value string) (submitMode, error) {
	switch submitMode(value) {
	case "":
		return submitModeLeader, nil
	case submitModeBroadcast:
		return submitModeBroadcast, nil
	case submitModeLeader:
		return submitModeLeader, nil
	default:
		return "", fmt.Errorf("unknown submit mode %q; expected %q or %q", value, submitModeBroadcast, submitModeLeader)
	}
}

func newNetworkSmallBankClient(hosts []hostEntry, requestTimeout time.Duration, mode submitMode, replyListen string, replyAdvertise string) (*networkSmallBankClient, error) {
	clients := make(map[uint64]*fabrictransport.Client, len(hosts))
	for _, host := range hosts {
		hostID := host.ID
		client, err := newSmallBankFabricClient(0, host.address(), clientQueueSize, func(operation string, err error) {
			fmt.Printf("SmallBank client stream to node %d failed: operation=%s err=%v\n", hostID, operation, err)
		})
		if err != nil {
			for _, existing := range clients {
				_ = existing.Close()
			}
			return nil, fmt.Errorf("create client transport to node %d at %s: %w", host.ID, host.address(), err)
		}
		clients[host.ID] = client
	}
	client := &networkSmallBankClient{
		hosts:          hosts,
		clients:        clients,
		requestTimeout: requestTimeout,
		submitMode:     mode,
		replyListen:    replyListen,
		replyAdvertise: replyAdvertise,
	}
	if mode == submitModeBroadcast {
		if err := client.startReplyServer(); err != nil {
			client.close()
			return nil, err
		}
		client.startSubmitSenders()
	}
	return client, nil
}

func (c *networkSmallBankClient) close() {
	for _, sender := range c.submitSenders {
		sender.close()
	}
	if c.replyServer != nil {
		c.replyServer.Stop()
	}
	if c.replyListener != nil {
		_ = c.replyListener.Close()
	}
	for id, client := range c.clients {
		if err := client.Close(); err != nil {
			fmt.Printf("SmallBank client transport close for node %d failed: %v\n", id, err)
		}
	}
}

func (c *networkSmallBankClient) startReplyServer() error {
	listenAddress := c.replyListen
	if listenAddress == "" {
		listenAddress = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return fmt.Errorf("listen for client replies: %w", err)
	}
	c.replyTracker = newReplyTracker(c.replyQuorum())
	c.replyListener = listener
	c.replyAddress = listener.Addr().String()
	if c.replyAdvertise != "" {
		_, port, err := net.SplitHostPort(c.replyAddress)
		if err != nil {
			_ = listener.Close()
			return fmt.Errorf("parse reply listener address %q: %w", c.replyAddress, err)
		}
		c.replyAddress = net.JoinHostPort(c.replyAdvertise, port)
	}
	c.replyServer = grpc.NewServer()
	if err := fabrictransport.RegisterServer(c.replyServer, &clientReplyServer{tracker: c.replyTracker}, fabrictransport.ServerConfig{
		SendTimeout: defaultNetworkSendTimeout,
		OnError: func(operation string, err error) {
			fmt.Printf("SmallBank client reply server stream failed: operation=%s err=%v\n", operation, err)
		},
	}); err != nil {
		_ = listener.Close()
		return err
	}
	go func() {
		_ = c.replyServer.Serve(listener)
	}()
	fmt.Printf("SmartBFT SmallBank client reply listener: address=%s quorum=%d\n", c.replyAddress, c.replyQuorum())
	return nil
}

func (c *networkSmallBankClient) startSubmitSenders() {
	c.submitSenders = make(map[uint64]*submitSender, len(c.hosts))
	for _, host := range c.hosts {
		c.submitSenders[host.ID] = newSubmitSender(host, c.clients[host.ID], c.replyAddress)
	}
}

func (c *networkSmallBankClient) invoke(ctx context.Context, req request) (response, error) {
	raw, err := encodeRequest(req)
	if err != nil {
		return response{}, err
	}

	if c.submitMode == submitModeBroadcast {
		return c.invokeBroadcast(ctx, req, raw)
	}
	return c.invokeLeader(ctx, raw)
}

func (c *networkSmallBankClient) invokeLeader(ctx context.Context, raw []byte) (response, error) {
	var lastErr error
	for attempt := 0; attempt < len(c.hosts)+1; attempt++ {
		host, ok := c.currentLeaderHost(ctx)
		if !ok {
			start := int(c.next.Add(1)-1) % len(c.hosts)
			host = c.hosts[start]
		}
		var out response
		err := callSmallBank(ctx, c.clients[host.ID], operationSubmit, &grpcSubmitRequest{
			Payload: raw,
			Mode:    string(submitModeLeader),
		}, &out)
		if err == nil {
			return out, nil
		}
		lastErr = fmt.Errorf("node %d submit failed: %w", host.ID, err)
		c.leader.Store(0)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no SmartBFT servers configured")
	}
	return response{}, lastErr
}

func (c *networkSmallBankClient) invokeBroadcast(ctx context.Context, req request, raw []byte) (response, error) {
	respCh, cancelWait := c.replyTracker.register(req)
	defer cancelWait()

	for _, host := range c.hosts {
		c.submitSenders[host.ID].enqueue(req, raw)
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-ctx.Done():
		return response{}, ctx.Err()
	}
}

func (c *networkSmallBankClient) replyQuorum() int {
	f := (len(c.hosts) - 1) / 3
	return f + 1
}

func (c *networkSmallBankClient) currentLeaderHost(ctx context.Context) (hostEntry, bool) {
	if leaderID := c.leader.Load(); leaderID != 0 {
		if host, ok := hostByID(c.hosts, leaderID); ok {
			return host, true
		}
	}
	if leaderID := c.refreshLeader(ctx); leaderID != 0 {
		if host, ok := hostByID(c.hosts, leaderID); ok {
			return host, true
		}
	}
	return hostEntry{}, false
}

func (c *networkSmallBankClient) refreshLeader(ctx context.Context) uint64 {
	for _, host := range c.hosts {
		statusCtx, cancel := context.WithTimeout(ctx, time.Second)
		var status grpcStatusResponse
		err := callSmallBank(statusCtx, c.clients[host.ID], operationStatus, &grpcStatusRequest{}, &status)
		cancel()
		if err != nil || !status.Running || status.Leader == 0 {
			continue
		}
		if _, ok := c.clients[status.Leader]; ok {
			c.leader.Store(status.Leader)
			return status.Leader
		}
	}
	return 0
}

func (c *networkSmallBankClient) waitForServers(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ready := 0
		leaderID := uint64(0)
		for _, host := range c.hosts {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			var status grpcStatusResponse
			err := callSmallBank(ctx, c.clients[host.ID], operationStatus, &grpcStatusRequest{}, &status)
			cancel()
			if err != nil {
				lastErr = err
				continue
			}
			if status.Running && status.Leader != 0 {
				leaderID = status.Leader
			}
			ready++
		}
		if ready == len(c.hosts) && leaderID != 0 {
			c.leader.Store(leaderID)
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		return fmt.Errorf("timed out waiting for SmartBFT servers: %w", lastErr)
	}
	return fmt.Errorf("timed out waiting for SmartBFT servers")
}

func (c *networkSmallBankClient) stateChecksums() map[uint64]string {
	checksums := make(map[uint64]string, len(c.hosts))
	for _, host := range c.hosts {
		ctx, cancel := context.WithTimeout(context.Background(), defaultNetworkSendTimeout)
		var checksum grpcChecksumResponse
		err := callSmallBank(ctx, c.clients[host.ID], operationChecksum, &grpcChecksumRequest{}, &checksum)
		cancel()
		if err != nil {
			checksums[host.ID] = fmt.Sprintf("ERROR:%v", err)
			continue
		}
		checksums[host.ID] = checksum.Checksum
	}
	return checksums
}

func callSmallBank(ctx context.Context, client *fabrictransport.Client, operation string, request any, response any) error {
	if client == nil {
		return fmt.Errorf("no client for operation %s", operation)
	}
	payload, err := fabrictransport.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode %s request: %w", operation, err)
	}
	raw, err := client.Call(ctx, operation, payload)
	if err != nil {
		return err
	}
	if response == nil {
		return nil
	}
	if err := fabrictransport.Unmarshal(raw, response); err != nil {
		return fmt.Errorf("decode %s response: %w", operation, err)
	}
	return nil
}
