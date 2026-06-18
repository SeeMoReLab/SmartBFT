// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	smart "github.com/hyperledger-labs/SmartBFT/pkg/api"
	"github.com/hyperledger-labs/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger-labs/SmartBFT/pkg/wal"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	defaultNetworkSendTimeout = 2 * time.Second
	smallBankNetworkService   = "smallbank.SmallBankNetwork"
	methodConsensus           = "/" + smallBankNetworkService + "/Consensus"
	methodTransaction         = "/" + smallBankNetworkService + "/Transaction"
	methodSubmit              = "/" + smallBankNetworkService + "/Submit"
	methodSubmitStream        = "/" + smallBankNetworkService + "/SubmitStream"
	methodStatus              = "/" + smallBankNetworkService + "/Status"
	methodChecksum            = "/" + smallBankNetworkService + "/Checksum"
	methodApplyTimeout        = "/" + smallBankNetworkService + "/ApplyTimeout"
	smallBankClientService    = "smallbank.SmallBankClient"
	methodClientReply         = "/" + smallBankClientService + "/Reply"
	methodClientReplyStream   = "/" + smallBankClientService + "/ReplyStream"
)

var smallBankCodec = smallBankGobCodec{}

func init() {
	encoding.RegisterCodec(smallBankCodec)
}

type smallBankGobCodec struct{}

func (smallBankGobCodec) Marshal(v any) ([]byte, error) {
	var out bytes.Buffer
	err := gob.NewEncoder(&out).Encode(v)
	return out.Bytes(), err
}

func (smallBankGobCodec) Unmarshal(data []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}

func (smallBankGobCodec) Name() string {
	return "smallbank-gob"
}

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

type grpcApplyTimeoutRequest struct {
	TimeoutMS int64
}

type grpcReplyRequest struct {
	From     uint64
	Response response
}

type smallBankNetworkServiceServer interface {
	Consensus(context.Context, *grpcConsensusRequest) (*grpcAck, error)
	Transaction(context.Context, *grpcTransactionRequest) (*grpcAck, error)
	Submit(context.Context, *grpcSubmitRequest) (*response, error)
	SubmitStream(smallBankSubmitStreamServer) error
	Status(context.Context, *grpcStatusRequest) (*grpcStatusResponse, error)
	Checksum(context.Context, *grpcChecksumRequest) (*grpcChecksumResponse, error)
	ApplyTimeout(context.Context, *grpcApplyTimeoutRequest) (*grpcAck, error)
}

type smallBankClientServiceServer interface {
	Reply(context.Context, *grpcReplyRequest) (*grpcAck, error)
	ReplyStream(smallBankClientReplyStreamServer) error
}

var smallBankNetworkServiceDesc = grpc.ServiceDesc{
	ServiceName: smallBankNetworkService,
	HandlerType: (*smallBankNetworkServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Consensus", Handler: smallBankConsensusHandler},
		{MethodName: "Transaction", Handler: smallBankTransactionHandler},
		{MethodName: "Submit", Handler: smallBankSubmitHandler},
		{MethodName: "Status", Handler: smallBankStatusHandler},
		{MethodName: "Checksum", Handler: smallBankChecksumHandler},
		{MethodName: "ApplyTimeout", Handler: smallBankApplyTimeoutHandler},
	},
	Streams: []grpc.StreamDesc{
		smallBankSubmitStreamDesc,
	},
}

var smallBankSubmitStreamDesc = grpc.StreamDesc{
	StreamName:    "SubmitStream",
	Handler:       smallBankSubmitStreamHandler,
	ClientStreams: true,
}

var smallBankClientServiceDesc = grpc.ServiceDesc{
	ServiceName: smallBankClientService,
	HandlerType: (*smallBankClientServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Reply", Handler: smallBankClientReplyHandler},
	},
	Streams: []grpc.StreamDesc{
		smallBankClientReplyStreamDesc,
	},
}

var smallBankClientReplyStreamDesc = grpc.StreamDesc{
	StreamName:    "ReplyStream",
	Handler:       smallBankClientReplyStreamHandler,
	ClientStreams: true,
}

func smallBankClientReplyHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(grpcReplyRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(smallBankClientServiceServer).Reply(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: methodClientReply}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(smallBankClientServiceServer).Reply(ctx, req.(*grpcReplyRequest))
	}
	return interceptor(ctx, in, info, handler)
}

type smallBankClientReplyStreamServer interface {
	Recv() (*grpcReplyRequest, error)
	grpc.ServerStream
}

type smallBankClientReplyStream struct {
	grpc.ServerStream
}

func (s *smallBankClientReplyStream) Recv() (*grpcReplyRequest, error) {
	req := new(grpcReplyRequest)
	if err := s.ServerStream.RecvMsg(req); err != nil {
		return nil, err
	}
	return req, nil
}

func smallBankClientReplyStreamHandler(srv any, stream grpc.ServerStream) error {
	return srv.(smallBankClientServiceServer).ReplyStream(&smallBankClientReplyStream{ServerStream: stream})
}

func smallBankConsensusHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(grpcConsensusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(smallBankNetworkServiceServer).Consensus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: methodConsensus}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(smallBankNetworkServiceServer).Consensus(ctx, req.(*grpcConsensusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func smallBankTransactionHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(grpcTransactionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(smallBankNetworkServiceServer).Transaction(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: methodTransaction}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(smallBankNetworkServiceServer).Transaction(ctx, req.(*grpcTransactionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func smallBankSubmitHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(grpcSubmitRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(smallBankNetworkServiceServer).Submit(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: methodSubmit}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(smallBankNetworkServiceServer).Submit(ctx, req.(*grpcSubmitRequest))
	}
	return interceptor(ctx, in, info, handler)
}

type smallBankSubmitStreamServer interface {
	Recv() (*grpcSubmitRequest, error)
	grpc.ServerStream
}

type smallBankSubmitStream struct {
	grpc.ServerStream
}

func (s *smallBankSubmitStream) Recv() (*grpcSubmitRequest, error) {
	req := new(grpcSubmitRequest)
	if err := s.ServerStream.RecvMsg(req); err != nil {
		return nil, err
	}
	return req, nil
}

func smallBankSubmitStreamHandler(srv any, stream grpc.ServerStream) error {
	return srv.(smallBankNetworkServiceServer).SubmitStream(&smallBankSubmitStream{ServerStream: stream})
}

func smallBankStatusHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(grpcStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(smallBankNetworkServiceServer).Status(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: methodStatus}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(smallBankNetworkServiceServer).Status(ctx, req.(*grpcStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func smallBankChecksumHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(grpcChecksumRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(smallBankNetworkServiceServer).Checksum(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: methodChecksum}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(smallBankNetworkServiceServer).Checksum(ctx, req.(*grpcChecksumRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func smallBankApplyTimeoutHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(grpcApplyTimeoutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(smallBankNetworkServiceServer).ApplyTimeout(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: methodApplyTimeout}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(smallBankNetworkServiceServer).ApplyTimeout(ctx, req.(*grpcApplyTimeoutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

type networkTransport struct {
	selfID uint64
	hosts  map[uint64]hostEntry
	peers  map[uint64]*grpc.ClientConn
	queues map[uint64]chan networkOutbound
	stop   chan struct{}
	wg     sync.WaitGroup
}

type networkOutbound struct {
	method  string
	request any
}

func newNetworkTransport(selfID uint64, hosts []hostEntry) (*networkTransport, error) {
	hostMap := make(map[uint64]hostEntry, len(hosts))
	for _, host := range hosts {
		hostMap[host.ID] = host
	}
	t := &networkTransport{
		selfID: selfID,
		hosts:  hostMap,
		peers:  make(map[uint64]*grpc.ClientConn),
		queues: make(map[uint64]chan networkOutbound),
		stop:   make(chan struct{}),
	}
	for _, host := range hosts {
		if host.ID == selfID {
			continue
		}
		conn, err := newSmallBankGRPCClientConn(host.address())
		if err != nil {
			t.close()
			return nil, fmt.Errorf("connect to node %d at %s: %w", host.ID, host.address(), err)
		}
		queue := make(chan networkOutbound, 4096)
		t.peers[host.ID] = conn
		t.queues[host.ID] = queue
		t.wg.Add(1)
		go t.worker(host, conn, queue)
	}
	return t, nil
}

func newSmallBankGRPCClientConn(target string) (*grpc.ClientConn, error) {
	return grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(smallBankCodec), grpc.WaitForReady(true)),
	)
}

func (t *networkTransport) close() {
	if t == nil {
		return
	}
	select {
	case <-t.stop:
	default:
		close(t.stop)
	}
	t.wg.Wait()
	for _, conn := range t.peers {
		_ = conn.Close()
	}
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
	raw, err := proto.Marshal(message)
	if err != nil {
		fmt.Printf("[network] marshal consensus message failed: target=%d err=%v\n", targetID, err)
		return
	}
	t.enqueue(targetID, networkOutbound{
		method: methodConsensus,
		request: &grpcConsensusRequest{
			From:    t.selfID,
			Message: raw,
		},
	})
}

func (t *networkTransport) sendTransaction(targetID uint64, payload []byte) {
	t.enqueue(targetID, networkOutbound{
		method: methodTransaction,
		request: &grpcTransactionRequest{
			From:    t.selfID,
			Payload: append([]byte(nil), payload...),
		},
	})
}

func (t *networkTransport) enqueue(targetID uint64, outbound networkOutbound) {
	queue, exists := t.queues[targetID]
	if !exists {
		fmt.Printf("[network] no route to target node %d\n", targetID)
		return
	}
	select {
	case queue <- outbound:
	case <-t.stop:
	}
}

func (t *networkTransport) worker(host hostEntry, conn *grpc.ClientConn, queue <-chan networkOutbound) {
	defer t.wg.Done()
	for {
		select {
		case <-t.stop:
			return
		case outbound := <-queue:
			t.invokeWithRetry(host, conn, outbound)
		}
	}
}

func (t *networkTransport) invokeWithRetry(host hostEntry, conn *grpc.ClientConn, outbound networkOutbound) {
	for {
		select {
		case <-t.stop:
			return
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), defaultNetworkSendTimeout)
		err := conn.Invoke(ctx, outbound.method, outbound.request, &grpcAck{})
		cancel()
		if err == nil {
			return
		}
		fmt.Printf("[network] send failed: target=%d method=%s err=%v\n", host.ID, outbound.method, err)
		select {
		case <-t.stop:
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
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

func (s *clientReplyServer) Reply(_ context.Context, req *grpcReplyRequest) (*grpcAck, error) {
	if req.From == 0 {
		return nil, grpcstatus.Error(codes.InvalidArgument, "missing sender id")
	}
	s.tracker.observe(req.From, req.Response)
	return &grpcAck{}, nil
}

func (s *clientReplyServer) ReplyStream(stream smallBankClientReplyStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if req.From == 0 {
			continue
		}
		s.tracker.observe(req.From, req.Response)
	}
}

type clientReplyDispatcher struct {
	selfID  uint64
	lock    sync.Mutex
	targets map[string]string
	senders map[string]*replySender
}

type replySender struct {
	address string
	conn    *grpc.ClientConn
	queue   chan response
	stop    chan struct{}
	wg      sync.WaitGroup
}

type submitSender struct {
	host         hostEntry
	conn         *grpc.ClientConn
	replyAddress string
	queue        chan broadcastSubmit
	stop         chan struct{}
	wg           sync.WaitGroup
}

type broadcastSubmit struct {
	req request
	raw []byte
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
			fmt.Printf("[network] create client reply sender failed: address=%s err=%v\n", address, err)
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
	conn, err := newSmallBankGRPCClientConn(address)
	if err != nil {
		return nil, err
	}
	s := &replySender{
		address: address,
		conn:    conn,
		queue:   make(chan response, 65536),
		stop:    make(chan struct{}),
	}
	workers := 4
	for i := 0; i < workers; i++ {
		s.wg.Add(1)
		go s.worker(selfID)
	}
	return s, nil
}

func (s *replySender) enqueue(resp response) {
	select {
	case s.queue <- resp:
	case <-s.stop:
	default:
		fmt.Printf("[network] client reply queue full: address=%s client=%s request=%s\n", s.address, resp.ClientID, resp.ID)
	}
}

func (s *replySender) worker(selfID uint64) {
	defer s.wg.Done()
	var stream grpc.ClientStream
	var cancel context.CancelFunc
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()
	for {
		select {
		case <-s.stop:
			return
		case resp := <-s.queue:
			if stream == nil {
				var err error
				stream, cancel, err = s.openStream()
				if err != nil {
					fmt.Printf("[network] open client reply stream failed: address=%s err=%v\n", s.address, err)
					s.sendUnary(selfID, resp)
					continue
				}
			}
			if err := stream.SendMsg(&grpcReplyRequest{From: selfID, Response: resp}); err != nil {
				if err != io.EOF && err != context.Canceled {
					fmt.Printf("[network] stream client reply failed: address=%s client=%s request=%s err=%v\n", s.address, resp.ClientID, resp.ID, err)
				}
				cancel()
				stream = nil
				cancel = nil
				s.sendUnary(selfID, resp)
			}
		}
	}
}

func (s *replySender) openStream() (grpc.ClientStream, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := s.conn.NewStream(ctx, &smallBankClientReplyStreamDesc, methodClientReplyStream)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return stream, cancel, nil
}

func (s *replySender) sendUnary(selfID uint64, resp response) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultNetworkSendTimeout)
	err := s.conn.Invoke(ctx, methodClientReply, &grpcReplyRequest{
		From:     selfID,
		Response: resp,
	}, &grpcAck{})
	cancel()
	if err != nil {
		fmt.Printf("[network] send client reply failed: address=%s client=%s request=%s err=%v\n", s.address, resp.ClientID, resp.ID, err)
	}
}

func (s *replySender) close() {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	s.wg.Wait()
	_ = s.conn.Close()
}

func newSubmitSender(host hostEntry, conn *grpc.ClientConn, replyAddress string) *submitSender {
	s := &submitSender{
		host:         host,
		conn:         conn,
		replyAddress: replyAddress,
		queue:        make(chan broadcastSubmit, 65536),
		stop:         make(chan struct{}),
	}
	workers := 2
	for i := 0; i < workers; i++ {
		s.wg.Add(1)
		go s.worker()
	}
	return s
}

func (s *submitSender) enqueue(req request, raw []byte) {
	submit := broadcastSubmit{
		req: req,
		raw: append([]byte(nil), raw...),
	}
	select {
	case s.queue <- submit:
	case <-s.stop:
	default:
		fmt.Printf("[network] broadcast submit queue full: node=%d client=%s request=%s\n", s.host.ID, req.ClientID, req.ID)
	}
}

func (s *submitSender) worker() {
	defer s.wg.Done()
	var stream grpc.ClientStream
	var cancel context.CancelFunc
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()
	for {
		select {
		case <-s.stop:
			return
		case submit := <-s.queue:
			if stream == nil {
				var err error
				stream, cancel, err = s.openStream()
				if err != nil {
					fmt.Printf("[network] open broadcast submit stream failed: node=%d err=%v\n", s.host.ID, err)
					s.sendUnary(submit)
					continue
				}
			}
			err := stream.SendMsg(&grpcSubmitRequest{
				Payload:      submit.raw,
				Mode:         string(submitModeBroadcast),
				ReplyAddress: s.replyAddress,
			})
			if err != nil {
				if err != io.EOF && err != context.Canceled {
					fmt.Printf("[network] stream broadcast submit failed: node=%d client=%s request=%s err=%v\n",
						s.host.ID, submit.req.ClientID, submit.req.ID, err)
				}
				cancel()
				stream = nil
				cancel = nil
				s.sendUnary(submit)
			}
		}
	}
}

func (s *submitSender) openStream() (grpc.ClientStream, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := s.conn.NewStream(ctx, &smallBankSubmitStreamDesc, methodSubmitStream)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return stream, cancel, nil
}

func (s *submitSender) sendUnary(submit broadcastSubmit) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultNetworkSendTimeout)
	var out response
	err := s.conn.Invoke(ctx, methodSubmit, &grpcSubmitRequest{
		Payload:      submit.raw,
		Mode:         string(submitModeBroadcast),
		ReplyAddress: s.replyAddress,
	}, &out)
	cancel()
	if err != nil {
		fmt.Printf("[network] broadcast submit failed: node=%d client=%s request=%s err=%v\n",
			s.host.ID, submit.req.ClientID, submit.req.ID, err)
	}
}

func (s *submitSender) close() {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	s.wg.Wait()
}

type networkNodeServer struct {
	node           *node
	host           hostEntry
	grpcServer     *grpc.Server
	listener       net.Listener
	requestTimeout time.Duration
}

func newNetworkNodeServer(
	id uint64,
	hosts []hostEntry,
	opts nodeOptions,
	dataDir string,
	verbose bool,
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
	logger := newSmallBankLogger(verbose)
	met := &disabled.Provider{}
	walMet := wal.NewMetrics(met, "smallbank")
	bftMet := smart.NewMetrics(met, "smallbank")

	in := make(chan wireMessage)
	pending := newPendingTracker()
	nodeOpts := opts
	nodeOpts.NumNodes = len(hosts)
	nodeOpts.Network = transport
	nodeOpts.Replies = newClientReplyDispatcher(id)
	learningOpts := opts.LearningOptions
	if learningOpts.Enabled {
		if learningOpts.NodeID == 0 {
			learningOpts.NodeID = 1
		}
		if learningOpts.NodeID == id {
			learningOpts.ApplyTimeout = applyRecommendedPBFTTimeoutOverNetwork(hosts)
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
		grpcServer:     grpc.NewServer(grpc.ForceServerCodec(smallBankCodec)),
		requestTimeout: requestTimeout,
	}
	s.grpcServer.RegisterService(&smallBankNetworkServiceDesc, s)
	return s, nil
}

func applyRecommendedPBFTTimeoutOverNetwork(hosts []hostEntry) func(time.Duration) error {
	conns := make(map[uint64]*grpc.ClientConn, len(hosts))
	return func(timeout time.Duration) error {
		var firstErr error
		for _, host := range hosts {
			conn := conns[host.ID]
			if conn == nil {
				var err error
				conn, err = newSmallBankGRPCClientConn(host.address())
				if err != nil {
					if firstErr == nil {
						firstErr = fmt.Errorf("node %d: %w", host.ID, err)
					}
					continue
				}
				conns[host.ID] = conn
			}
			ctx, cancel := context.WithTimeout(context.Background(), defaultNetworkSendTimeout)
			err := conn.Invoke(ctx, methodApplyTimeout, &grpcApplyTimeoutRequest{TimeoutMS: timeout.Milliseconds()}, &grpcAck{})
			cancel()
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("node %d: %w", host.ID, err)
			}
		}
		return firstErr
	}
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

func (s *networkNodeServer) Consensus(_ context.Context, req *grpcConsensusRequest) (*grpcAck, error) {
	msg := &smartbftprotos.Message{}
	if err := proto.Unmarshal(req.Message, msg); err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "decode consensus message: %v", err)
	}
	s.node.consensus.HandleMessage(req.From, msg)
	return &grpcAck{}, nil
}

func (s *networkNodeServer) Transaction(_ context.Context, req *grpcTransactionRequest) (*grpcAck, error) {
	s.node.consensus.HandleRequest(req.From, req.Payload)
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

	if err := s.node.consensus.SubmitRequest(req.Payload); err != nil {
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

func (s *networkNodeServer) SubmitStream(stream smallBankSubmitStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		decoded, err := decodeRequest(req.Payload)
		if err != nil {
			fmt.Printf("[network] decode broadcast submit failed: node=%d err=%v\n", s.node.id, err)
			continue
		}
		if err := s.acceptBroadcastSubmit(decoded, req); err != nil {
			fmt.Printf("[network] accept broadcast submit failed: node=%d client=%s request=%s err=%v\n",
				s.node.id, decoded.ClientID, decoded.ID, err)
		}
	}
}

func (s *networkNodeServer) acceptBroadcastSubmit(decoded request, req *grpcSubmitRequest) error {
	if req.ReplyAddress == "" {
		return fmt.Errorf("broadcast submit missing reply address")
	}
	s.node.replies.remember(decoded, req.ReplyAddress)
	s.node.pending.markSubmitted(decoded)
	return s.node.consensus.SubmitRequest(req.Payload)
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
	raw := mustJSON(struct {
		Accounts map[uint64]string `json:"accounts"`
		Checking map[uint64]int64  `json:"checking"`
		Savings  map[uint64]int64  `json:"savings"`
	}{
		Accounts: s.node.state.accounts,
		Checking: s.node.state.checking,
		Savings:  s.node.state.savings,
	})
	s.node.stateLock.Unlock()
	return &grpcChecksumResponse{NodeID: s.node.id, Checksum: hashBytes(raw)}, nil
}

func (s *networkNodeServer) ApplyTimeout(_ context.Context, req *grpcApplyTimeoutRequest) (*grpcAck, error) {
	if req.TimeoutMS <= 0 {
		return nil, grpcstatus.Error(codes.InvalidArgument, "timeout_ms must be positive")
	}
	config, err := s.node.consensus.ApplyRequestTimeout(time.Duration(req.TimeoutMS) * time.Millisecond)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "apply timeout: %v", err)
	}
	s.node.configuration = config
	fmt.Printf("[learning] applied SmartBFT request timeout: node=%d total_timeout_ms=%d forward_timeout_ms=%d complain_timeout_ms=%d\n",
		s.node.id, req.TimeoutMS, config.RequestForwardTimeout.Milliseconds(), config.RequestComplainTimeout.Milliseconds())
	return &grpcAck{}, nil
}

type networkSmallBankClient struct {
	hosts          []hostEntry
	conns          map[uint64]*grpc.ClientConn
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
	conns := make(map[uint64]*grpc.ClientConn, len(hosts))
	for _, host := range hosts {
		conn, err := newSmallBankGRPCClientConn(host.address())
		if err != nil {
			for _, existing := range conns {
				_ = existing.Close()
			}
			return nil, fmt.Errorf("connect to node %d at %s: %w", host.ID, host.address(), err)
		}
		conns[host.ID] = conn
	}
	client := &networkSmallBankClient{
		hosts:          hosts,
		conns:          conns,
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
	for _, conn := range c.conns {
		_ = conn.Close()
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
	c.replyServer = grpc.NewServer(grpc.ForceServerCodec(smallBankCodec))
	c.replyServer.RegisterService(&smallBankClientServiceDesc, &clientReplyServer{tracker: c.replyTracker})
	go func() {
		if err := c.replyServer.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			fmt.Printf("[network] client reply server failed: %v\n", err)
		}
	}()
	fmt.Printf("SmartBFT SmallBank client reply listener: address=%s quorum=%d\n", c.replyAddress, c.replyQuorum())
	return nil
}

func (c *networkSmallBankClient) startSubmitSenders() {
	c.submitSenders = make(map[uint64]*submitSender, len(c.hosts))
	for _, host := range c.hosts {
		c.submitSenders[host.ID] = newSubmitSender(host, c.conns[host.ID], c.replyAddress)
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
		err := c.conns[host.ID].Invoke(ctx, methodSubmit, &grpcSubmitRequest{
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
		err := c.conns[host.ID].Invoke(statusCtx, methodStatus, &grpcStatusRequest{}, &status)
		cancel()
		if err != nil || !status.Running || status.Leader == 0 {
			continue
		}
		if _, ok := c.conns[status.Leader]; ok {
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
			err := c.conns[host.ID].Invoke(ctx, methodStatus, &grpcStatusRequest{}, &status)
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
		err := c.conns[host.ID].Invoke(ctx, methodChecksum, &grpcChecksumRequest{}, &checksum)
		cancel()
		if err != nil {
			checksums[host.ID] = fmt.Sprintf("ERROR:%v", err)
			continue
		}
		checksums[host.ID] = checksum.Checksum
	}
	return checksums
}
