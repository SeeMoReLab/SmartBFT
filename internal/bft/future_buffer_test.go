// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync/atomic"
	"testing"

	"github.com/hyperledger-labs/SmartBFT/pkg/api"
	"github.com/hyperledger-labs/SmartBFT/pkg/metrics/disabled"
	protos "github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"go.uber.org/zap"
)

func TestFutureMessageBufferPromotesCurrentAndNextSequence(t *testing.T) {
	view := futureBufferTestView(1)

	futurePrePrepare := futureBufferPrePrepare(1, 3)
	if !view.bufferFutureMessage(1, futurePrePrepare, 1, 3) {
		t.Fatal("expected future pre-prepare to be buffered")
	}

	view.startNextSeq()

	if view.ProposalSequence != 2 {
		t.Fatalf("expected proposal sequence 2, got %d", view.ProposalSequence)
	}
	if len(view.futureMessages) != 0 {
		t.Fatalf("expected future buffer to be empty, got %d entries", len(view.futureMessages))
	}
	if len(view.nextPrePrepare) != 1 {
		t.Fatalf("expected promoted next pre-prepare, got channel length %d", len(view.nextPrePrepare))
	}
	if seq := (<-view.nextPrePrepare).GetPrePrepare().Seq; seq != 3 {
		t.Fatalf("expected promoted pre-prepare seq 3, got %d", seq)
	}
}

func TestFutureMessageBufferKeepsOnlyWindowedConsensusMessages(t *testing.T) {
	view := futureBufferTestView(1)

	if !view.bufferFutureMessage(1, futureBufferPrePrepare(1, 11), 1, 11) {
		t.Fatal("expected sequence at the edge of the future window to be buffered")
	}
	if view.bufferFutureMessage(1, futureBufferPrePrepare(1, 12), 1, 12) {
		t.Fatal("expected sequence beyond the future window to be rejected")
	}
	if view.bufferFutureMessage(1, futureBufferPrePrepare(2, 3), 2, 3) {
		t.Fatal("expected different-view message to be rejected")
	}
	if view.bufferFutureMessage(1, &protos.Message{}, 1, 3) {
		t.Fatal("expected non-consensus message to be rejected")
	}
}

func futureBufferTestView(proposalSeq uint64) *View {
	view := &View{
		N:                4,
		LeaderID:         1,
		Quorum:           3,
		Number:           1,
		ProposalSequence: proposalSeq,
		Logger:           zap.NewNop().Sugar(),
		MetricsView:      api.NewMetricsView(&disabled.Provider{}),
		ViewSequences:    &atomic.Value{},
	}
	view.prePrepare = make(chan *protos.Message, 1)
	view.nextPrePrepare = make(chan *protos.Message, 1)
	view.futureMessages = make(map[uint64]map[futureMessageKey]bufferedFutureMessage)
	view.setupVotes()
	return view
}

func futureBufferPrePrepare(view uint64, seq uint64) *protos.Message {
	return &protos.Message{
		Content: &protos.Message_PrePrepare{
			PrePrepare: &protos.PrePrepare{
				View: view,
				Seq:  seq,
				Proposal: &protos.Proposal{
					Metadata: MarshalOrPanic(&protos.ViewMetadata{
						ViewId:         view,
						LatestSequence: seq,
					}),
				},
			},
		},
	}
}
