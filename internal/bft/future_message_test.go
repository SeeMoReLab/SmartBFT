// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync/atomic"
	"testing"

	protos "github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"go.uber.org/zap"
)

type countingSynchronizer struct {
	calls atomic.Uint32
}

func (s *countingSynchronizer) Sync() {
	s.calls.Add(1)
}

func TestOutOfRangeCommitsTriggerSync(t *testing.T) {
	synchronizer := &countingSynchronizer{}
	view := &View{
		SelfID:                4,
		N:                     4,
		LeaderID:              1,
		Number:                1,
		ProposalSequence:      1,
		Logger:                zap.NewNop().Sugar(),
		Sync:                  synchronizer,
		lastVotedProposalByID: make(map[uint64]*protos.Commit),
		abortChan:             make(chan struct{}),
	}
	view.stopReason.Store("running")

	commit := func(sender uint64) {
		view.processMsg(sender, &protos.Message{
			Content: &protos.Message_Commit{
				Commit: &protos.Commit{
					View:   1,
					Seq:    3,
					Digest: "future-decision",
				},
			},
		})
	}

	commit(1)
	if got := synchronizer.calls.Load(); got != 0 {
		t.Fatalf("expected one future commit to be below the sync threshold, got %d sync calls", got)
	}

	commit(2)
	if got := synchronizer.calls.Load(); got != 1 {
		t.Fatalf("expected f+1 matching future commits to trigger one sync, got %d", got)
	}
	if !view.Stopped() {
		t.Fatal("expected the view to stop before synchronizing")
	}
}
