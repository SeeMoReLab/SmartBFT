// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"sync"
	"testing"
	"time"
)

type sendRecorder struct {
	mu    sync.Mutex
	sent  []string
	times map[string]time.Time
}

func newSendRecorder() *sendRecorder {
	return &sendRecorder{times: make(map[string]time.Time)}
}

func (r *sendRecorder) record(name string) func() {
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.sent = append(r.sent, name)
		r.times[name] = time.Now()
	}
}

func (r *sendRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.sent...)
}

func waitForSends(t *testing.T, r *sendRecorder, want int, within time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		sent := r.snapshot()
		if len(sent) >= want {
			return sent
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %d sends within %s, got %v", want, within, sent)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestProposalSendLineSendsInlineWhenIdle(t *testing.T) {
	line := newProposalSendLine(nil)
	defer line.stop()
	rec := newSendRecorder()
	line.submit(1, 0, rec.record("a"))
	if got := rec.snapshot(); len(got) != 1 || got[0] != "a" {
		t.Fatalf("idle submit must send inline, got %v", got)
	}
}

func TestProposalSendLineKeepsOrderBehindDelayedProposal(t *testing.T) {
	line := newProposalSendLine(nil)
	defer line.stop()
	rec := newSendRecorder()
	const delay = 150 * time.Millisecond
	start := time.Now()
	line.submit(1, delay, rec.record("pre_prepare"))
	line.submit(1, 0, rec.record("prepare"))
	line.submit(1, 0, rec.record("commit"))
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("nothing may be sent before the delay elapses, got %v", got)
	}
	sent := waitForSends(t, rec, 3, 2*time.Second)
	if sent[0] != "pre_prepare" || sent[1] != "prepare" || sent[2] != "commit" {
		t.Fatalf("messages left the line out of order: %v", sent)
	}
	rec.mu.Lock()
	elapsed := rec.times["pre_prepare"].Sub(start)
	rec.mu.Unlock()
	if elapsed < delay {
		t.Fatalf("pre_prepare released after %s, before the %s delay", elapsed, delay)
	}
}

func TestProposalSendLineDropsHeldMessagesOfOlderView(t *testing.T) {
	var dropMu sync.Mutex
	var dropped []uint64
	line := newProposalSendLine(func(view uint64) {
		dropMu.Lock()
		defer dropMu.Unlock()
		dropped = append(dropped, view)
	})
	defer line.stop()
	rec := newSendRecorder()
	line.submit(1, 5*time.Second, rec.record("stale_pre_prepare"))
	line.submit(1, 0, rec.record("stale_prepare"))
	// The replica moved on and votes in the next view; the held view-1
	// messages must be discarded promptly and the view-2 vote sent.
	line.submit(2, 0, rec.record("prepare_view2"))
	sent := waitForSends(t, rec, 1, time.Second)
	if len(sent) != 1 || sent[0] != "prepare_view2" {
		t.Fatalf("expected only the view-2 message to be sent, got %v", sent)
	}
	dropMu.Lock()
	defer dropMu.Unlock()
	if len(dropped) != 2 || dropped[0] != 1 || dropped[1] != 1 {
		t.Fatalf("expected both view-1 messages dropped, got %v", dropped)
	}
}

func TestProposalSendLineStopReleasesWaiter(t *testing.T) {
	line := newProposalSendLine(nil)
	rec := newSendRecorder()
	line.submit(1, time.Hour, rec.record("never"))
	done := make(chan struct{})
	go func() {
		line.stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop did not return while a delayed message was held")
	}
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("held message must not be sent after stop, got %v", got)
	}
}
