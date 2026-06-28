// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"testing"

	protos "github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"google.golang.org/protobuf/proto"
)

func TestViewChangerCoalescesPendingViewMessages(t *testing.T) {
	vc := &ViewChanger{
		SelfID:          1,
		incMsgs:         make(chan *incMsg, 4),
		pendingViewMsgs: make(map[viewMessageCoalesceKey]struct{}),
		stopChan:        make(chan struct{}),
		currView:        1,
		nextView:        1,
		realView:        1,
	}

	msg := viewChangeMessage(2)
	vc.HandleMessage(2, msg)
	vc.HandleMessage(2, msg)

	if got := len(vc.incMsgs); got != 1 {
		t.Fatalf("expected one coalesced pending message, got %d", got)
	}
	queued := <-vc.incMsgs
	vc.releasePendingViewMessage(queued.coalesce)

	vc.HandleMessage(2, msg)
	if got := len(vc.incMsgs); got != 1 {
		t.Fatalf("expected message after pending key release, got %d", got)
	}
}

func TestViewChangerDropsStaleViewMessages(t *testing.T) {
	vc := &ViewChanger{
		SelfID:          1,
		incMsgs:         make(chan *incMsg, 4),
		pendingViewMsgs: make(map[viewMessageCoalesceKey]struct{}),
		stopChan:        make(chan struct{}),
		currView:        4,
		nextView:        4,
		realView:        3,
	}

	vc.HandleMessage(2, viewChangeMessage(3))
	if got := len(vc.incMsgs); got != 0 {
		t.Fatalf("expected stale message to be dropped, got queue length %d", got)
	}

	vc.HandleMessage(2, viewChangeMessage(4))
	if got := len(vc.incMsgs); got != 1 {
		t.Fatalf("expected non-stale message to be enqueued, got %d", got)
	}
}

func TestViewChangerKeepsActiveViewData(t *testing.T) {
	vc := &ViewChanger{
		SelfID:          1,
		incMsgs:         make(chan *incMsg, 4),
		pendingViewMsgs: make(map[viewMessageCoalesceKey]struct{}),
		stopChan:        make(chan struct{}),
		currView:        1,
		nextView:        1,
		realView:        1,
	}

	msg, err := viewDataMessage(1)
	if err != nil {
		t.Fatal(err)
	}
	vc.HandleMessage(2, msg)

	if got := len(vc.incMsgs); got != 1 {
		t.Fatalf("expected active view data to be enqueued, got %d", got)
	}
}

func viewChangeMessage(nextView uint64) *protos.Message {
	return &protos.Message{
		Content: &protos.Message_ViewChange{
			ViewChange: &protos.ViewChange{NextView: nextView},
		},
	}
}

func viewDataMessage(nextView uint64) (*protos.Message, error) {
	raw, err := proto.Marshal(&protos.ViewData{NextView: nextView})
	if err != nil {
		return nil, err
	}
	return &protos.Message{
		Content: &protos.Message_ViewData{
			ViewData: &protos.SignedViewData{RawViewData: raw},
		},
	}, nil
}
