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
	"sync"
	"time"

	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/proto"
)

const (
	reportChainService = "sharing.ReportChain"
	methodConsensus    = "/" + reportChainService + "/Consensus"
	methodTransaction  = "/" + reportChainService + "/Transaction"
	methodSubmit       = "/" + reportChainService + "/Submit"
	methodLatest       = "/" + reportChainService + "/Latest"

	reportQueueSize   = 1024
	reportSendTimeout = 2 * time.Second
)

var sharingCodec = sharingGobCodec{}

func init() {
	encoding.RegisterCodec(sharingCodec)
}

// sharingGobCodec keeps the peer transport free of generated protobuf types.
// The report chain payloads are plain Go structs; protobuf messages inside them
// are carried pre-marshaled as bytes.
type sharingGobCodec struct{}

func (sharingGobCodec) Marshal(v any) ([]byte, error) {
	var out bytes.Buffer
	err := gob.NewEncoder(&out).Encode(v)
	return out.Bytes(), err
}

func (sharingGobCodec) Unmarshal(data []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}

func (sharingGobCodec) Name() string {
	return "sharing-gob"
}

type reportAck struct{}

type reportConsensusRequest struct {
	From    uint64
	Message []byte
}

type reportTransactionRequest struct {
	From    uint64
	Payload []byte
}

// reportSubmitRequest carries a marshaled ReportBatch that a peer's learning
// agent handed to its local sharing server. Submissions are broadcast so the
// current leader learns about an episode without waiting for SmartBFT's
// leader-forwarding timeout.
type reportSubmitRequest struct {
	From    uint64
	Payload []byte
}

// latestDecisionRequest asks a peer for its tip of the report chain.
type latestDecisionRequest struct{}

type reportChainServer interface {
	Consensus(context.Context, *reportConsensusRequest) (*reportAck, error)
	Transaction(context.Context, *reportTransactionRequest) (*reportAck, error)
	Submit(context.Context, *reportSubmitRequest) (*reportAck, error)
	Latest(context.Context, *latestDecisionRequest) (*latestDecisionResponse, error)
}

var reportChainServiceDesc = grpc.ServiceDesc{
	ServiceName: reportChainService,
	HandlerType: (*reportChainServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Consensus", Handler: reportConsensusHandler},
		{MethodName: "Transaction", Handler: reportTransactionHandler},
		{MethodName: "Submit", Handler: reportSubmitHandler},
		{MethodName: "Latest", Handler: reportLatestHandler},
	},
	Streams: []grpc.StreamDesc{},
}

func reportLatestHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := new(latestDecisionRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(reportChainServer).Latest(ctx, req)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: methodLatest}
	return interceptor(ctx, req, info, func(ctx context.Context, req any) (any, error) {
		return srv.(reportChainServer).Latest(ctx, req.(*latestDecisionRequest))
	})
}

func reportConsensusHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := new(reportConsensusRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(reportChainServer).Consensus(ctx, req)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: methodConsensus}
	return interceptor(ctx, req, info, func(ctx context.Context, req any) (any, error) {
		return srv.(reportChainServer).Consensus(ctx, req.(*reportConsensusRequest))
	})
}

func reportTransactionHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := new(reportTransactionRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(reportChainServer).Transaction(ctx, req)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: methodTransaction}
	return interceptor(ctx, req, info, func(ctx context.Context, req any) (any, error) {
		return srv.(reportChainServer).Transaction(ctx, req.(*reportTransactionRequest))
	})
}

func reportSubmitHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	req := new(reportSubmitRequest)
	if err := dec(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(reportChainServer).Submit(ctx, req)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: methodSubmit}
	return interceptor(ctx, req, info, func(ctx context.Context, req any) (any, error) {
		return srv.(reportChainServer).Submit(ctx, req.(*reportSubmitRequest))
	})
}

type outbound struct {
	method  string
	request any
}

// reportTransport is a per-peer queue of report-chain messages. The report
// chain carries one small batch per node per episode, so it needs neither the
// message coalescing nor the async forwarding that the workload transport uses.
type reportTransport struct {
	selfID   uint64
	hosts    map[uint64]hostEntry
	ids      []uint64
	peers    map[uint64]*grpc.ClientConn
	queues   map[uint64]chan outbound
	stop     chan struct{}
	stopOnce sync.Once
	workerWG sync.WaitGroup
}

func newReportTransport(selfID uint64, hosts []hostEntry) (*reportTransport, error) {
	t := &reportTransport{
		selfID: selfID,
		hosts:  make(map[uint64]hostEntry, len(hosts)),
		ids:    nodeIDs(hosts),
		peers:  make(map[uint64]*grpc.ClientConn),
		queues: make(map[uint64]chan outbound),
		stop:   make(chan struct{}),
	}
	for _, host := range hosts {
		t.hosts[host.ID] = host
	}
	for _, host := range hosts {
		if host.ID == selfID {
			continue
		}
		conn, err := newSharingClientConn(host.reportAddress())
		if err != nil {
			t.close()
			return nil, fmt.Errorf("connect to sharing node %d at %s: %w", host.ID, host.reportAddress(), err)
		}
		queue := make(chan outbound, reportQueueSize)
		t.peers[host.ID] = conn
		t.queues[host.ID] = queue
		t.workerWG.Add(1)
		go t.worker(host, conn, queue)
	}
	return t, nil
}

func newSharingClientConn(target string) (*grpc.ClientConn, error) {
	return grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(sharingCodec), grpc.WaitForReady(true)),
	)
}

func (t *reportTransport) close() {
	if t == nil {
		return
	}
	t.stopOnce.Do(func() { close(t.stop) })
	t.workerWG.Wait()
	for _, conn := range t.peers {
		_ = conn.Close()
	}
}

func (t *reportTransport) nodeIDs() []uint64 {
	ids := make([]uint64, len(t.ids))
	copy(ids, t.ids)
	return ids
}

func (t *reportTransport) sendConsensus(targetID uint64, message *smartbftprotos.Message) {
	raw, err := proto.Marshal(message)
	if err != nil {
		return
	}
	t.enqueue(targetID, outbound{
		method:  methodConsensus,
		request: &reportConsensusRequest{From: t.selfID, Message: raw},
	})
}

func (t *reportTransport) sendTransaction(targetID uint64, payload []byte) {
	t.enqueue(targetID, outbound{
		method:  methodTransaction,
		request: &reportTransactionRequest{From: t.selfID, Payload: append([]byte(nil), payload...)},
	})
}

// broadcastSubmit hands a locally submitted batch to every peer so that
// whichever node currently leads the report chain can propose it immediately.
func (t *reportTransport) broadcastSubmit(payload []byte) {
	for _, id := range t.ids {
		if id == t.selfID {
			continue
		}
		t.enqueue(id, outbound{
			method:  methodSubmit,
			request: &reportSubmitRequest{From: t.selfID, Payload: append([]byte(nil), payload...)},
		})
	}
}

func (t *reportTransport) enqueue(targetID uint64, out outbound) {
	queue, exists := t.queues[targetID]
	if !exists {
		return
	}
	select {
	case queue <- out:
	case <-t.stop:
	default:
		fmt.Printf("%s dropped message to sharing node %d: queue full method=%s\n",
			logTag("transport"), targetID, out.method)
	}
}

func (t *reportTransport) worker(host hostEntry, conn *grpc.ClientConn, queue <-chan outbound) {
	defer t.workerWG.Done()
	for {
		select {
		case <-t.stop:
			return
		case out := <-queue:
			ctx, cancel := context.WithTimeout(context.Background(), reportSendTimeout)
			err := conn.Invoke(ctx, out.method, out.request, &reportAck{})
			cancel()
			if err != nil {
				fmt.Printf("%s send to sharing node %d failed: method=%s err=%v\n",
					logTag("transport"), host.ID, out.method, err)
			}
		}
	}
}
