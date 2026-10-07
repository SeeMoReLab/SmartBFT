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
	operationClientReply      = "smallbank-client-reply"
)

// Static HTTP/2 flow-control windows for node-to-node connections. A SmartBFT
// new-view message carries 2f+1 signed view-data entries, each holding the last
// decision and the in-flight proposal, so it reaches a few hundred KB with
// 256-transaction batches. With gRPC's default 64KB window such a message needs
// several round trips, which under a 50ms one-way delay is longer than the
// view-change timeout. The windows below let one new-view plus the following
// pre-prepare travel in a single round trip.
const (
	consensusStreamWindowSize     = 4 << 20
	consensusConnectionWindowSize = 8 << 20
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

type grpcConsensusRequest struct {
	Message []byte
}

type grpcTransactionRequest struct {
	Payload []byte
}

type grpcSubmitRequest struct {
	Payload      []byte
	ReplyAddress string
}

type grpcStatusRequest struct{}

type grpcStatusResponse struct {
	Leader  uint64
	Running bool
}

type grpcChecksumRequest struct{}

type grpcChecksumResponse struct {
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

type grpcReplyRequest struct {
	Response response
}

type networkTransport struct {
	selfID    uint64
	hosts     map[uint64]hostEntry
	clients   map[uint64]*fabrictransport.Client
	failures  *transportFailureLog
	closeOnce sync.Once
}

// transportFailureLog prints failures on the replica-to-replica transport at
// most once per second per peer and operation, with the number of failures
// suppressed since the previous line. Peer 0 means the peer is unknown, as for
// a failed inbound stream.
type transportFailureLog struct {
	selfID     uint64
	lock       sync.Mutex
	lastPrint  map[transportFailureKey]time.Time
	suppressed map[transportFailureKey]int
}

type transportFailureKey struct {
	peer      uint64
	operation string
}

func newTransportFailureLog(selfID uint64) *transportFailureLog {
	return &transportFailureLog{
		selfID:     selfID,
		lastPrint:  make(map[transportFailureKey]time.Time),
		suppressed: make(map[transportFailureKey]int),
	}
}

func (l *transportFailureLog) report(peer uint64, operation string, err error) {
	key := transportFailureKey{peer: peer, operation: operation}
	now := time.Now()
	l.lock.Lock()
	if last, printed := l.lastPrint[key]; printed && now.Sub(last) < time.Second {
		l.suppressed[key]++
		l.lock.Unlock()
		return
	}
	suppressed := l.suppressed[key]
	l.suppressed[key] = 0
	l.lastPrint[key] = now
	l.lock.Unlock()
	fmt.Printf("%s transport failure: node=%d peer=%d operation=%s suppressed=%d err=%v\n",
		timestampedLogTag("transport"), l.selfID, peer, operation, suppressed, err)
}

func newNetworkTransport(selfID uint64, hosts []hostEntry) (*networkTransport, error) {
	t := &networkTransport{
		selfID:   selfID,
		hosts:    make(map[uint64]hostEntry, len(hosts)),
		clients:  make(map[uint64]*fabrictransport.Client),
		failures: newTransportFailureLog(selfID),
	}
	for _, host := range hosts {
		t.hosts[host.ID] = host
		if host.ID == selfID {
			continue
		}
		peerID := host.ID
		client, err := newSmallBankFabricClient(selfID, host.address(), consensusQueueSize, consensusFlowControl, func(operation string, err error) {
			t.failures.report(peerID, operation, err)
		})
		if err != nil {
			t.close()
			return nil, fmt.Errorf("create transport to node %d at %s: %w", host.ID, host.address(), err)
		}
		t.clients[host.ID] = client
	}
	return t, nil
}

// flowControlWindows selects the HTTP/2 flow-control windows a client
// advertises. Zero values keep gRPC's defaults.
type flowControlWindows struct {
	stream     int32
	connection int32
}

var (
	// consensusFlowControl is used for node-to-node connections, which carry
	// large view-change and proposal messages.
	consensusFlowControl = flowControlWindows{stream: consensusStreamWindowSize, connection: consensusConnectionWindowSize}
	// defaultFlowControl is used for client submit and reply connections,
	// whose messages are individual transactions and fit the default window.
	defaultFlowControl = flowControlWindows{}
)

func newSmallBankFabricClient(
	selfID uint64,
	target string,
	queueSize int,
	windows flowControlWindows,
	onError func(operation string, err error),
) (*fabrictransport.Client, error) {
	return fabrictransport.NewClient(fabrictransport.ClientConfig{
		SelfID:                selfID,
		Address:               target,
		QueueSize:             queueSize,
		SendTimeout:           defaultNetworkSendTimeout,
		MaxReceiveMsgSize:     syncMaxReceiveMessageSize,
		InitialWindowSize:     windows.stream,
		InitialConnWindowSize: windows.connection,
		OnError:               onError,
	})
}

func (t *networkTransport) close() {
	if t == nil {
		return
	}
	t.closeOnce.Do(func() {
		for id, client := range t.clients {
			if err := client.Close(); err != nil {
				fmt.Printf("%s transport close failed: node=%d peer=%d err=%v\n", timestampedLogTag("transport"), t.selfID, id, err)
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
		t.failures.report(targetID, operationConsensus, errors.New("no route to peer"))
		return
	}
	messageRaw, err := proto.Marshal(message)
	if err != nil {
		t.failures.report(targetID, operationConsensus, fmt.Errorf("marshal message: %w", err))
		return
	}
	payload, err := fabrictransport.Marshal(&grpcConsensusRequest{Message: messageRaw})
	if err != nil {
		t.failures.report(targetID, operationConsensus, fmt.Errorf("marshal envelope: %w", err))
		return
	}
	if err := client.Send(operationConsensus, payload, true); err != nil {
		t.failures.report(targetID, operationConsensus, err)
	}
}

func (t *networkTransport) sendTransaction(targetID uint64, raw []byte) {
	client := t.clients[targetID]
	if client == nil {
		t.failures.report(targetID, operationTransaction, errors.New("no route to peer"))
		return
	}
	payload, err := fabrictransport.Marshal(&grpcTransactionRequest{Payload: append([]byte(nil), raw...)})
	if err != nil {
		t.failures.report(targetID, operationTransaction, fmt.Errorf("marshal envelope: %w", err))
		return
	}
	if err := client.Send(operationTransaction, payload, false); err != nil {
		t.failures.report(targetID, operationTransaction, err)
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

	raw, err := client.Call(rpcCtx, operationStateTransfer, request)
	if err != nil {
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
	return snapshot, nil
}

type replyTracker struct {
	lock    sync.Mutex
	quorum  int
	waiters map[string]*replyWaiter
}

type replyWaiter struct {
	ch       chan response
	matches  map[response]int
	received map[uint64]struct{}
}

func newReplyTracker(quorum int) *replyTracker {
	return &replyTracker{
		quorum:  quorum,
		waiters: make(map[string]*replyWaiter),
	}
}

func (t *replyTracker) register(req request) (<-chan response, func()) {
	key := requestKey(req.ClientID, req.ID)
	ch := make(chan response, 1)

	t.lock.Lock()
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
	s.tracker.observe(from, request.Response)
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
	client, err := newSmallBankFabricClient(selfID, address, clientQueueSize, defaultFlowControl, func(operation string, err error) {
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
	if err := s.client.Send(operationClientReply, payload, true); err != nil {
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

func (s *submitSender) enqueue(raw []byte) {
	payload, err := fabrictransport.Marshal(&grpcSubmitRequest{
		Payload:      append([]byte(nil), raw...),
		ReplyAddress: s.replyAddress,
	})
	if err != nil {
		fmt.Printf("SmallBank submit encoding for node %d failed: %v\n", s.host.ID, err)
		return
	}
	if err := s.client.Send(operationSubmit, payload, false); err != nil {
		fmt.Printf("SmallBank submit enqueue for node %d failed: %v\n", s.host.ID, err)
	}
}

type networkNodeServer struct {
	node       *node
	host       hostEntry
	grpcServer *grpc.Server
	listener   net.Listener
	closeOnce  sync.Once
}

func newNetworkNodeServer(
	id uint64,
	hosts []hostEntry,
	opts nodeOptions,
	dataDir string,
	logMode smallBankLogMode,
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

	pending := newPendingTracker()
	nodeOpts := opts
	nodeOpts.Network = transport
	nodeOpts.Replies = newClientReplyDispatcher(id)
	var localNode *node
	learningOpts := opts.LearningOptions
	if learningOpts.Enabled {
		learningOpts.NodeID = id
		learningOpts.ApplyTimeout = func(timeout time.Duration) error {
			if localNode == nil {
				return fmt.Errorf("node %d not initialized", id)
			}
			return localNode.applyBaseRequestTimeout(timeout)
		}
	}
	learning, err := newLearningManager(learningOpts)
	if err != nil {
		transport.close()
		return nil, err
	}
	nodeOpts.Learning = learning
	n, err := newNode(id, pending, logger, walMet, bftMet, nodeOpts, dataDir)
	if err != nil {
		learning.close()
		transport.close()
		return nil, err
	}
	localNode = n

	// The node server accepts consensus and state-transfer streams from peers,
	// so its receive windows must match the peers' consensusFlowControl.
	serverOptions, err := fabrictransport.FlowControlServerOptions(consensusFlowControl.stream, consensusFlowControl.connection)
	if err != nil {
		n.stop()
		transport.close()
		return nil, err
	}
	listener, err := net.Listen("tcp", host.address())
	if err != nil {
		n.stop()
		transport.close()
		return nil, err
	}
	s := &networkNodeServer{
		node:       n,
		host:       host,
		listener:   listener,
		grpcServer: grpc.NewServer(serverOptions...),
	}
	if err := fabrictransport.RegisterServer(s.grpcServer, s, fabrictransport.ServerConfig{
		SendTimeout: defaultNetworkSendTimeout,
		OnError: func(operation string, err error) {
			transport.failures.report(0, operation, err)
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
func (s *networkNodeServer) Handle(_ context.Context, from uint64, operation string, payload []byte) ([]byte, error) {
	switch operation {
	case operationConsensus:
		request := new(grpcConsensusRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode consensus request: %w", err)
		}
		return nil, s.handleConsensusRequest(from, request)
	case operationTransaction:
		request := new(grpcTransactionRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode forwarded transaction: %w", err)
		}
		s.Transaction(from, request)
		return nil, nil
	case operationSubmit:
		request := new(grpcSubmitRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode client submission: %w", err)
		}
		decoded, err := decodeRequest(request.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode broadcast submission: %w", err)
		}
		if err := s.acceptBroadcastSubmit(decoded, request); err != nil {
			return nil, err
		}
		return fabrictransport.Marshal(&response{ClientID: decoded.ClientID, ID: decoded.ID, Status: statusSuccess})
	case operationStatus:
		request := new(grpcStatusRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode status request: %w", err)
		}
		return fabrictransport.Marshal(s.Status())
	case operationChecksum:
		request := new(grpcChecksumRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode checksum request: %w", err)
		}
		return fabrictransport.Marshal(s.Checksum())
	case operationStateTransfer:
		request := new(grpcStateSnapshotRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode state-transfer request: %w", err)
		}
		result, err := s.StateSnapshot(request)
		if err != nil {
			return nil, err
		}
		return fabrictransport.Marshal(result)
	default:
		return nil, fmt.Errorf("unknown SmallBank transport operation %q", operation)
	}
}

func (s *networkNodeServer) handleConsensusRequest(from uint64, req *grpcConsensusRequest) error {
	msg := &smartbftprotos.Message{}
	if err := proto.Unmarshal(req.Message, msg); err != nil {
		return grpcstatus.Errorf(codes.InvalidArgument, "decode consensus message: %v", err)
	}
	s.node.consensus.HandleMessage(from, msg)
	return nil
}

// Transaction admits a request forwarded by another replica. A rejection is not
// reported back: the forwarder does not wait for an answer.
func (s *networkNodeServer) Transaction(from uint64, req *grpcTransactionRequest) {
	_ = s.node.handleRequest(from, req.Payload)
}

func (s *networkNodeServer) acceptBroadcastSubmit(decoded request, req *grpcSubmitRequest) error {
	if req.ReplyAddress == "" {
		return fmt.Errorf("broadcast submit missing reply address")
	}
	s.node.replies.remember(decoded, req.ReplyAddress)
	s.node.pending.markSubmitted(decoded)
	return s.node.submitRequest(decoded.ClientID, decoded.ID, req.Payload)
}

func (s *networkNodeServer) Status() *grpcStatusResponse {
	return &grpcStatusResponse{
		Leader:  s.node.consensus.GetLeaderID(),
		Running: s.node.consensus.GetLeaderID() != 0,
	}
}

func (s *networkNodeServer) Checksum() *grpcChecksumResponse {
	s.node.stateLock.Lock()
	raw := mustJSON(s.node.state.deterministicStateSnapshot())
	s.node.stateLock.Unlock()
	return &grpcChecksumResponse{Checksum: hashBytes(raw)}
}

func (s *networkNodeServer) StateSnapshot(req *grpcStateSnapshotRequest) (*stateSyncSnapshot, error) {
	snapshot, err := s.node.serveSyncSnapshot(*req)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.FailedPrecondition, "serve state snapshot: %v", err)
	}
	return &snapshot, nil
}

type networkSmallBankClient struct {
	hosts          []hostEntry
	clients        map[uint64]*fabrictransport.Client
	replyListen    string
	replyAdvertise string
	replyTracker   *replyTracker
	replyAddress   string
	replyServer    *grpc.Server
	replyListener  net.Listener
	submitSenders  map[uint64]*submitSender
}

func newNetworkSmallBankClient(hosts []hostEntry, replyListen string, replyAdvertise string) (*networkSmallBankClient, error) {
	clients := make(map[uint64]*fabrictransport.Client, len(hosts))
	for _, host := range hosts {
		hostID := host.ID
		client, err := newSmallBankFabricClient(0, host.address(), clientQueueSize, defaultFlowControl, func(operation string, err error) {
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
		replyListen:    replyListen,
		replyAdvertise: replyAdvertise,
	}
	if err := client.startReplyServer(); err != nil {
		client.close()
		return nil, err
	}
	client.startSubmitSenders()
	return client, nil
}

func (c *networkSmallBankClient) close() {
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

	respCh, cancelWait := c.replyTracker.register(req)
	defer cancelWait()

	for _, host := range c.hosts {
		c.submitSenders[host.ID].enqueue(raw)
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
