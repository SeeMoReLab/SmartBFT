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
	consensusQueueSize        = 4096
	consensusStreamRetryDelay = 100 * time.Millisecond
	asyncForwardLimit         = 1024
	smallBankNetworkService   = "smallbank.SmallBankNetwork"
	methodConsensusStream     = "/" + smallBankNetworkService + "/ConsensusStream"
	methodTransaction         = "/" + smallBankNetworkService + "/Transaction"
	methodSubmit              = "/" + smallBankNetworkService + "/Submit"
	methodSubmitStream        = "/" + smallBankNetworkService + "/SubmitStream"
	methodStatus              = "/" + smallBankNetworkService + "/Status"
	methodChecksum            = "/" + smallBankNetworkService + "/Checksum"
	methodStateSnapshot       = "/" + smallBankNetworkService + "/StateSnapshot"
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

type grpcStateSnapshotRequest struct{}

type grpcApplyTimeoutRequest struct {
	TimeoutMS int64
}

type grpcReplyRequest struct {
	From     uint64
	Response response
}

type smallBankNetworkServiceServer interface {
	ConsensusStream(smallBankConsensusStreamServer) error
	Transaction(context.Context, *grpcTransactionRequest) (*grpcAck, error)
	Submit(context.Context, *grpcSubmitRequest) (*response, error)
	SubmitStream(smallBankSubmitStreamServer) error
	Status(context.Context, *grpcStatusRequest) (*grpcStatusResponse, error)
	Checksum(context.Context, *grpcChecksumRequest) (*grpcChecksumResponse, error)
	StateSnapshot(context.Context, *grpcStateSnapshotRequest) (*stateSyncSnapshot, error)
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
		{MethodName: "Transaction", Handler: smallBankTransactionHandler},
		{MethodName: "Submit", Handler: smallBankSubmitHandler},
		{MethodName: "Status", Handler: smallBankStatusHandler},
		{MethodName: "Checksum", Handler: smallBankChecksumHandler},
		{MethodName: "StateSnapshot", Handler: smallBankStateSnapshotHandler},
		{MethodName: "ApplyTimeout", Handler: smallBankApplyTimeoutHandler},
	},
	Streams: []grpc.StreamDesc{
		smallBankConsensusStreamDesc,
		smallBankSubmitStreamDesc,
	},
}

var smallBankConsensusStreamDesc = grpc.StreamDesc{
	StreamName:    "ConsensusStream",
	Handler:       smallBankConsensusStreamHandler,
	ClientStreams: true,
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

type smallBankConsensusStreamServer interface {
	Recv() (*grpcConsensusRequest, error)
	grpc.ServerStream
}

type smallBankConsensusStream struct {
	grpc.ServerStream
}

func (s *smallBankConsensusStream) Recv() (*grpcConsensusRequest, error) {
	req := new(grpcConsensusRequest)
	if err := s.ServerStream.RecvMsg(req); err != nil {
		return nil, err
	}
	return req, nil
}

func smallBankConsensusStreamHandler(srv any, stream grpc.ServerStream) error {
	return srv.(smallBankNetworkServiceServer).ConsensusStream(&smallBankConsensusStream{ServerStream: stream})
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

func smallBankStateSnapshotHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(grpcStateSnapshotRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(smallBankNetworkServiceServer).StateSnapshot(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: methodStateSnapshot}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(smallBankNetworkServiceServer).StateSnapshot(ctx, req.(*grpcStateSnapshotRequest))
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
	selfID          uint64
	hosts           map[uint64]hostEntry
	peers           map[uint64]*grpc.ClientConn
	queues          map[uint64]chan networkOutbound
	pending         map[uint64]map[networkCoalesceKey]struct{}
	pendingLock     sync.Mutex
	forwardInflight map[uint64]chan struct{}
	streamContext   context.Context
	cancelStreams   context.CancelFunc
	stop            chan struct{}
	workerWG        sync.WaitGroup
	asyncWG         sync.WaitGroup
}

type networkOutbound struct {
	method   string
	request  any
	coalesce *networkCoalesceKey
	trace    string
}

type networkCoalesceKey struct {
	kind string
	view uint64
}

func smallBankRequestTrace(raw []byte) string {
	req, err := decodeRequest(raw)
	if err != nil {
		return fmt.Sprintf("request_decode_error=%q request_bytes=%d", err.Error(), len(raw))
	}
	return fmt.Sprintf("request={%s %s}", req.ClientID, req.ID)
}

func smartBFTNetworkCoalesceKey(message *smartbftprotos.Message) *networkCoalesceKey {
	kind, view, ok := smartBFTViewMessageTarget(message)
	if !ok {
		return nil
	}
	return &networkCoalesceKey{kind: kind, view: view}
}

func smartBFTViewMessageTarget(message *smartbftprotos.Message) (string, uint64, bool) {
	if message == nil {
		return "", 0, false
	}
	if vc := message.GetViewChange(); vc != nil {
		return "view_change", vc.GetNextView(), true
	}
	if vd := message.GetViewData(); vd != nil {
		target, ok := smartBFTSignedViewDataTarget(vd)
		return "view_data", target, ok
	}
	if nv := message.GetNewView(); nv != nil {
		for _, svd := range nv.GetSignedViewData() {
			target, ok := smartBFTSignedViewDataTarget(svd)
			if ok {
				return "new_view", target, true
			}
		}
		return "new_view", 0, false
	}
	return "", 0, false
}

func smartBFTSignedViewDataTarget(svd *smartbftprotos.SignedViewData) (uint64, bool) {
	if svd == nil {
		return 0, false
	}
	vd := &smartbftprotos.ViewData{}
	if err := proto.Unmarshal(svd.GetRawViewData(), vd); err != nil {
		return 0, false
	}
	return vd.GetNextView(), true
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
	hostMap := make(map[uint64]hostEntry, len(hosts))
	for _, host := range hosts {
		hostMap[host.ID] = host
	}
	streamContext, cancelStreams := context.WithCancel(context.Background())
	t := &networkTransport{
		selfID:          selfID,
		hosts:           hostMap,
		peers:           make(map[uint64]*grpc.ClientConn),
		queues:          make(map[uint64]chan networkOutbound),
		pending:         make(map[uint64]map[networkCoalesceKey]struct{}),
		forwardInflight: make(map[uint64]chan struct{}),
		streamContext:   streamContext,
		cancelStreams:   cancelStreams,
		stop:            make(chan struct{}),
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
		queue := make(chan networkOutbound, consensusQueueSize)
		t.peers[host.ID] = conn
		t.queues[host.ID] = queue
		t.pending[host.ID] = make(map[networkCoalesceKey]struct{})
		t.forwardInflight[host.ID] = make(chan struct{}, asyncForwardLimit)
		t.workerWG.Add(1)
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
	t.cancelStreams()
	t.workerWG.Wait()
	t.asyncWG.Wait()
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
		return
	}
	key := smartBFTNetworkCoalesceKey(message)
	trace := smartBFTTraceMessageSummary(message)
	t.enqueue(targetID, networkOutbound{
		method:   methodConsensusStream,
		coalesce: key,
		trace:    trace,
		request: &grpcConsensusRequest{
			From:    t.selfID,
			Message: raw,
		},
	})
}

func (t *networkTransport) sendTransaction(targetID uint64, payload []byte) {
	trace := smallBankRequestTrace(payload)
	t.enqueue(targetID, networkOutbound{
		method: methodTransaction,
		trace:  trace,
		request: &grpcTransactionRequest{
			From:    t.selfID,
			Payload: append([]byte(nil), payload...),
		},
	})
}

func (t *networkTransport) enqueue(targetID uint64, outbound networkOutbound) {
	queue, exists := t.queues[targetID]
	if !exists {
		return
	}
	if !t.reservePending(targetID, outbound.coalesce) {
		if outbound.method == methodConsensusStream {
			smallbankTracePrintf("%s event=send_consensus_drop_coalesced node=%d to=%d queue_len=%d queue_cap=%d %s\n",
				timestampedLogTag("trace"), t.selfID, targetID, len(queue), cap(queue), outbound.trace)
		}
		return
	}
	if outbound.method == methodConsensusStream {
		smallbankTracePrintf("%s event=send_consensus_enqueue_start node=%d to=%d queue_len=%d queue_cap=%d %s\n",
			timestampedLogTag("trace"), t.selfID, targetID, len(queue), cap(queue), outbound.trace)
	} else if outbound.method == methodTransaction {
		smallbankTracePrintf("%s event=forward_enqueue_start node=%d to=%d queue_len=%d queue_cap=%d %s\n",
			timestampedLogTag("trace"), t.selfID, targetID, len(queue), cap(queue), outbound.trace)
	}
	select {
	case queue <- outbound:
		if outbound.method == methodConsensusStream {
			smallbankTracePrintf("%s event=send_consensus_enqueue_done node=%d to=%d queue_len=%d queue_cap=%d %s\n",
				timestampedLogTag("trace"), t.selfID, targetID, len(queue), cap(queue), outbound.trace)
		} else if outbound.method == methodTransaction {
			smallbankTracePrintf("%s event=forward_enqueue_done node=%d to=%d queue_len=%d queue_cap=%d %s\n",
				timestampedLogTag("trace"), t.selfID, targetID, len(queue), cap(queue), outbound.trace)
		}
	case <-t.stop:
		t.releasePending(targetID, outbound.coalesce)
		if outbound.method == methodConsensusStream {
			smallbankTracePrintf("%s event=send_consensus_enqueue_stopped node=%d to=%d %s\n",
				timestampedLogTag("trace"), t.selfID, targetID, outbound.trace)
		} else if outbound.method == methodTransaction {
			smallbankTracePrintf("%s event=forward_enqueue_stopped node=%d to=%d %s\n",
				timestampedLogTag("trace"), t.selfID, targetID, outbound.trace)
		}
	}
}

func (t *networkTransport) worker(host hostEntry, conn *grpc.ClientConn, queue <-chan networkOutbound) {
	defer t.workerWG.Done()
	var consensusStream grpc.ClientStream
	defer func() {
		if consensusStream != nil {
			_ = consensusStream.CloseSend()
		}
	}()

	for {
		select {
		case <-t.stop:
			return
		case outbound := <-queue:
			if outbound.method == methodConsensusStream {
				smallbankTracePrintf("%s event=send_consensus_dequeue node=%d to=%d queue_len=%d queue_cap=%d %s\n",
					timestampedLogTag("trace"), t.selfID, host.ID, len(queue), cap(queue), outbound.trace)
			} else if outbound.method == methodTransaction {
				smallbankTracePrintf("%s event=forward_dequeue node=%d to=%d queue_len=%d queue_cap=%d %s\n",
					timestampedLogTag("trace"), t.selfID, host.ID, len(queue), cap(queue), outbound.trace)
			}

			if outbound.method == methodConsensusStream {
				sent := t.sendConsensusStream(host, conn, outbound, &consensusStream)
				t.releasePending(host.ID, outbound.coalesce)
				if !sent {
					return
				}
				continue
			}

			t.releasePending(host.ID, outbound.coalesce)
			if outbound.method == methodTransaction {
				t.invokeForwardAsync(host, conn, outbound)
				continue
			}
			t.invokeOnce(host, conn, outbound)
		}
	}
}

func (t *networkTransport) reservePending(targetID uint64, key *networkCoalesceKey) bool {
	if key == nil {
		return true
	}
	t.pendingLock.Lock()
	defer t.pendingLock.Unlock()
	pending := t.pending[targetID]
	if pending == nil {
		pending = make(map[networkCoalesceKey]struct{})
		t.pending[targetID] = pending
	}
	if _, exists := pending[*key]; exists {
		return false
	}
	pending[*key] = struct{}{}
	return true
}

func (t *networkTransport) releasePending(targetID uint64, key *networkCoalesceKey) {
	if key == nil {
		return
	}
	t.pendingLock.Lock()
	if pending := t.pending[targetID]; pending != nil {
		delete(pending, *key)
	}
	t.pendingLock.Unlock()
}

func (t *networkTransport) sendConsensusStream(
	host hostEntry,
	conn *grpc.ClientConn,
	outbound networkOutbound,
	stream *grpc.ClientStream,
) bool {
	for {
		if *stream == nil {
			opened, err := conn.NewStream(t.streamContext, &smallBankConsensusStreamDesc, methodConsensusStream)
			if err != nil {
				smallbankTracePrintf("%s event=send_consensus_stream_open_failed node=%d to=%d err=%q %s\n",
					timestampedLogTag("trace"), t.selfID, host.ID, err.Error(), outbound.trace)
				if !t.waitForConsensusStreamRetry() {
					return false
				}
				continue
			}
			*stream = opened
			smallbankTracePrintf("%s event=send_consensus_stream_opened node=%d to=%d\n",
				timestampedLogTag("trace"), t.selfID, host.ID)
		}

		start := time.Now()
		err := (*stream).SendMsg(outbound.request)
		if err == nil {
			smallbankTracePrintf("%s event=send_consensus_stream_sent node=%d to=%d elapsed_ms=%d %s\n",
				timestampedLogTag("trace"), t.selfID, host.ID, time.Since(start).Milliseconds(), outbound.trace)
			return true
		}

		smallbankTracePrintf("%s event=send_consensus_stream_failed node=%d to=%d elapsed_ms=%d err=%q %s\n",
			timestampedLogTag("trace"), t.selfID, host.ID, time.Since(start).Milliseconds(), err.Error(), outbound.trace)
		_ = (*stream).CloseSend()
		*stream = nil
		if !t.waitForConsensusStreamRetry() {
			return false
		}
	}
}

func (t *networkTransport) waitForConsensusStreamRetry() bool {
	timer := time.NewTimer(consensusStreamRetryDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-t.stop:
		return false
	}
}

func (t *networkTransport) invokeForwardAsync(host hostEntry, conn *grpc.ClientConn, outbound networkOutbound) {
	inflight, exists := t.forwardInflight[host.ID]
	if !exists {
		smallbankTracePrintf("%s event=forward_drop node=%d to=%d reason=no_inflight_bucket %s\n",
			timestampedLogTag("trace"), t.selfID, host.ID, outbound.trace)
		return
	}
	select {
	case inflight <- struct{}{}:
	case <-t.stop:
		smallbankTracePrintf("%s event=forward_drop node=%d to=%d reason=transport_stopped %s\n",
			timestampedLogTag("trace"), t.selfID, host.ID, outbound.trace)
		return
	default:
		smallbankTracePrintf("%s event=forward_drop node=%d to=%d reason=inflight_full inflight_len=%d inflight_cap=%d %s\n",
			timestampedLogTag("trace"), t.selfID, host.ID, len(inflight), cap(inflight), outbound.trace)
		return
	}

	t.asyncWG.Add(1)
	go func() {
		defer t.asyncWG.Done()
		defer func() { <-inflight }()
		t.invokeOnce(host, conn, outbound)
	}()
}

func (t *networkTransport) invokeOnce(host hostEntry, conn *grpc.ClientConn, outbound networkOutbound) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultNetworkSendTimeout)
	start := time.Now()
	if outbound.method == methodTransaction {
		smallbankTracePrintf("%s event=forward_rpc_start node=%d to=%d method=%s %s\n",
			timestampedLogTag("trace"), t.selfID, host.ID, outbound.method, outbound.trace)
	}
	err := conn.Invoke(ctx, outbound.method, outbound.request, &grpcAck{})
	cancel()
	if outbound.method == methodTransaction {
		if err != nil {
			smallbankTracePrintf("%s event=forward_rpc_done node=%d to=%d method=%s elapsed_ms=%d err=%q %s\n",
				timestampedLogTag("trace"), t.selfID, host.ID, outbound.method, time.Since(start).Milliseconds(), err.Error(), outbound.trace)
		} else {
			smallbankTracePrintf("%s event=forward_rpc_done node=%d to=%d method=%s elapsed_ms=%d err=\"\" %s\n",
				timestampedLogTag("trace"), t.selfID, host.ID, outbound.method, time.Since(start).Milliseconds(), outbound.trace)
		}
	}
	if err == nil {
		return
	}
}

func (t *networkTransport) fetchStateSnapshot(targetID uint64) (stateSyncSnapshot, error) {
	conn, exists := t.peers[targetID]
	if !exists {
		return stateSyncSnapshot{}, fmt.Errorf("no route to target node %d", targetID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultNetworkSendTimeout)
	defer cancel()

	start := time.Now()
	smallbankTracePrintf("%s event=sync_rpc_start node=%d to=%d method=StateSnapshot\n",
		timestampedLogTag("trace"), t.selfID, targetID)
	var snapshot stateSyncSnapshot
	if err := conn.Invoke(ctx, methodStateSnapshot, &grpcStateSnapshotRequest{}, &snapshot); err != nil {
		smallbankTracePrintf("%s event=sync_rpc_done node=%d to=%d method=StateSnapshot elapsed_ms=%d err=%q\n",
			timestampedLogTag("trace"), t.selfID, targetID, time.Since(start).Milliseconds(), err.Error())
		return stateSyncSnapshot{}, err
	}
	smallbankTracePrintf("%s event=sync_rpc_done node=%d to=%d method=StateSnapshot elapsed_ms=%d err=\"\" view=%d seq=%d source=%d\n",
		timestampedLogTag("trace"), t.selfID, targetID, time.Since(start).Milliseconds(), snapshot.View, snapshot.Sequence, snapshot.NodeID)
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
					s.sendUnary(selfID, resp)
					continue
				}
			}
			if err := stream.SendMsg(&grpcReplyRequest{From: selfID, Response: resp}); err != nil {
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
	_ = s.conn.Invoke(ctx, methodClientReply, &grpcReplyRequest{
		From:     selfID,
		Response: resp,
	}, &grpcAck{})
	cancel()
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
	_ = err
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
		grpcServer:     grpc.NewServer(grpc.ForceServerCodec(smallBankCodec)),
		requestTimeout: requestTimeout,
	}
	s.grpcServer.RegisterService(&smallBankNetworkServiceDesc, s)
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

func (s *networkNodeServer) ConsensusStream(stream smallBankConsensusStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := s.handleConsensusRequest(req); err != nil {
			return err
		}
	}
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
			continue
		}
		_ = s.acceptBroadcastSubmit(decoded, req)
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

func (s *networkNodeServer) StateSnapshot(context.Context, *grpcStateSnapshotRequest) (*stateSyncSnapshot, error) {
	start := time.Now()
	smallbankTracePrintf("%s event=recv_sync_start node=%d method=StateSnapshot\n",
		timestampedLogTag("trace"), s.node.id)
	snapshot := s.node.localStateSyncSnapshot()
	smallbankTracePrintf("%s event=recv_sync_done node=%d method=StateSnapshot elapsed_ms=%d view=%d seq=%d\n",
		timestampedLogTag("trace"), s.node.id, time.Since(start).Milliseconds(), snapshot.View, snapshot.Sequence)
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
		_ = c.replyServer.Serve(listener)
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
