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
	"sync"

	gogotypes "github.com/gogo/protobuf/types"
	"github.com/hyperledger-labs/SmartBFT/examples/internal/fabrictransport"
	algorithm "github.com/hyperledger-labs/SmartBFT/internal/bft"
	smart "github.com/hyperledger-labs/SmartBFT/pkg/api"
	adaptivetimers "github.com/hyperledger-labs/SmartBFT/proto/adaptive_timers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// sharingServer is one node of the report chain. It listens on two ports:
//
//	reportPort    peer-to-peer report chain traffic, gob encoded
//	consensusPort Consensus/SubmitReportBatch from the local learning agent
//
// The two are kept apart so that agent traffic and consensus traffic never
// share a codec, a connection pool, or a failure mode.
type sharingServer struct {
	node  *reportNode
	agent *agentClient
	host  hostEntry

	peerServer    *grpc.Server
	peerListener  net.Listener
	agentServer   *grpc.Server
	agentListener net.Listener

	closeOnce sync.Once
	serveWG   sync.WaitGroup
	adaptivetimers.UnimplementedConsensusServer
}

type sharingOptions struct {
	ID       uint64
	Hosts    []hostEntry
	Protocol adaptivetimers.Protocol
	DataDir  string
	Logger   smart.Logger
}

func newSharingServer(opts sharingOptions) (*sharingServer, error) {
	host, exists := hostByID(opts.Hosts, opts.ID)
	if !exists {
		return nil, fmt.Errorf("sharing node id %d not found in hosts config", opts.ID)
	}

	transport, err := newReportTransport(opts.ID, opts.Hosts)
	if err != nil {
		return nil, err
	}

	agent, err := newAgentClient(opts.ID, host.agentAddress())
	if err != nil {
		transport.close()
		return nil, err
	}

	node, err := newReportNode(reportNodeOptions{
		ID:        opts.ID,
		Hosts:     opts.Hosts,
		Protocol:  opts.Protocol,
		DataDir:   opts.DataDir,
		Logger:    opts.Logger,
		Transport: transport,
		Agent:     agent,
	})
	if err != nil {
		agent.close()
		transport.close()
		return nil, err
	}

	peerListener, err := net.Listen("tcp", host.reportAddress())
	if err != nil {
		node.stop()
		agent.close()
		transport.close()
		return nil, fmt.Errorf("listen for report chain peers on %s: %w", host.reportAddress(), err)
	}
	agentListener, err := net.Listen("tcp", host.consensusAddress())
	if err != nil {
		_ = peerListener.Close()
		node.stop()
		agent.close()
		transport.close()
		return nil, fmt.Errorf("listen for learning agent on %s: %w", host.consensusAddress(), err)
	}

	s := &sharingServer{
		node:          node,
		agent:         agent,
		host:          host,
		peerServer:    grpc.NewServer(),
		peerListener:  peerListener,
		agentServer:   grpc.NewServer(),
		agentListener: agentListener,
	}
	if err := fabrictransport.RegisterServer(s.peerServer, s, fabrictransport.ServerConfig{
		SendTimeout: reportSendTimeout,
		OnError: func(operation string, err error) {
			fmt.Printf("%s peer stream failed: node=%d operation=%s err=%v\n", logTag("transport"), host.ID, operation, err)
		},
	}); err != nil {
		_ = peerListener.Close()
		_ = agentListener.Close()
		node.stop()
		agent.close()
		transport.close()
		return nil, err
	}
	adaptivetimers.RegisterConsensusServer(s.agentServer, s)
	return s, nil
}

// Handle dispatches every non-agent request received on the shared
// Fabric-style Step service. The envelope's sender ID replaces the redundant
// sender field that the old unary RPC transport trusted.
func (s *sharingServer) Handle(ctx context.Context, from uint64, operation string, payload []byte) ([]byte, error) {
	switch operation {
	case operationReportConsensus:
		request := new(reportConsensusRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode report consensus request: %w", err)
		}
		request.From = from
		_, err := s.Consensus(ctx, request)
		return nil, err
	case operationReportTransaction:
		request := new(reportTransactionRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode report transaction request: %w", err)
		}
		request.From = from
		_, err := s.Transaction(ctx, request)
		return nil, err
	case operationReportSubmit:
		request := new(reportSubmitRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode report submission: %w", err)
		}
		request.From = from
		_, err := s.Submit(ctx, request)
		return nil, err
	case operationReportStateTransfer:
		request := new(latestDecisionRequest)
		if err := fabrictransport.Unmarshal(payload, request); err != nil {
			return nil, fmt.Errorf("decode report state-transfer request: %w", err)
		}
		response, err := s.Latest(ctx, request)
		if err != nil {
			return nil, err
		}
		return fabrictransport.Marshal(response)
	default:
		return nil, fmt.Errorf("unknown sharing transport operation %q", operation)
	}
}

func (s *sharingServer) serve() error {
	fmt.Printf("%s sharing server listening: node=%d peers=%s agent=%s\n",
		logTag("sharing"), s.host.ID, s.host.reportAddress(), s.host.consensusAddress())

	errCh := make(chan error, 2)
	s.serveWG.Add(2)
	go func() {
		defer s.serveWG.Done()
		errCh <- s.peerServer.Serve(s.peerListener)
	}()
	go func() {
		defer s.serveWG.Done()
		errCh <- s.agentServer.Serve(s.agentListener)
	}()

	err := <-errCh
	if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

func (s *sharingServer) close() {
	s.closeOnce.Do(func() {
		s.peerServer.Stop()
		s.agentServer.Stop()
		s.node.stop()
		s.node.transport.close()
		s.agent.close()
		s.serveWG.Wait()
	})
}

// --- Consensus service, called by the local learning agent ---

func (s *sharingServer) SubmitReportBatch(_ context.Context, batch *adaptivetimers.ReportBatch) (*gogotypes.Empty, error) {
	raw, err := encodeReportBatch(batch)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "marshal report batch: %v", err)
	}
	if _, err := s.node.VerifyRequest(raw); err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "%v", err)
	}

	accepted := s.submitLocal(raw, batch.Episode, "agent")
	// Broadcast regardless of the local outcome: a peer that has not seen this
	// episode still needs it, and the current leader may not be this node.
	s.node.transport.broadcastSubmit(raw)
	if accepted {
		fmt.Printf("%s submitted: node=%d episode=%d reports=%d contributors=%v\n",
			logTag("sharing"), s.host.ID, batch.Episode, len(batch.Reports), contributorIDs(batch))
	}
	return &gogotypes.Empty{}, nil
}

// submitLocal reports whether the batch entered the pool. Duplicate episodes are
// the normal case, not an error: every node submits the same episode and only
// one submission needs to survive.
func (s *sharingServer) submitLocal(raw []byte, episode uint32, source string) bool {
	err := s.node.submit(raw)
	switch {
	case err == nil:
		return true
	case errors.Is(err, algorithm.ErrReqAlreadyExists), errors.Is(err, algorithm.ErrReqAlreadyProcessed):
		return false
	default:
		fmt.Printf("%s submit rejected: node=%d episode=%d source=%s err=%v\n",
			logTag("sharing"), s.host.ID, episode, source, err)
		return false
	}
}

// --- ReportChain service, called by peer sharing servers ---

func (s *sharingServer) Consensus(_ context.Context, req *reportConsensusRequest) (*reportAck, error) {
	if err := s.node.handleConsensusMessage(req.From, req.Message); err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "%v", err)
	}
	return &reportAck{}, nil
}

func (s *sharingServer) Transaction(_ context.Context, req *reportTransactionRequest) (*reportAck, error) {
	if err := s.node.handleForwardedRequest(req.From, req.Payload); err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "%v", err)
	}
	return &reportAck{}, nil
}

func (s *sharingServer) Submit(_ context.Context, req *reportSubmitRequest) (*reportAck, error) {
	batch, err := decodeReportBatch(req.Payload)
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "%v", err)
	}
	if _, err := s.node.VerifyRequest(req.Payload); err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "%v", err)
	}
	s.submitLocal(req.Payload, batch.Episode, fmt.Sprintf("node-%d", req.From))
	return &reportAck{}, nil
}

func (s *sharingServer) Latest(_ context.Context, _ *latestDecisionRequest) (*latestDecisionResponse, error) {
	decision, ok := s.node.latestDecision()
	if !ok {
		return &latestDecisionResponse{NodeID: s.host.ID}, nil
	}
	view, sequence, valid := decisionViewSequence(decision)
	if !valid {
		return &latestDecisionResponse{NodeID: s.host.ID}, nil
	}
	return &latestDecisionResponse{
		NodeID:   s.host.ID,
		HaveTip:  true,
		View:     view,
		Sequence: sequence,
		Decision: decision,
	}, nil
}
