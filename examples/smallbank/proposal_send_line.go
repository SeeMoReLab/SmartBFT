// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"sync"
	"time"

	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
)

// proposalSendLine holds a replica's outgoing consensus messages to one peer
// while an injected proposal delay is in effect, without blocking any
// consensus goroutine. The fault is meant to delay proposals only: the replica
// keeps voting, joining view changes, syncing, and running its timers while
// its proposal is held back.
//
// Ordering: messages submitted to the line leave it in submission order, so a
// delayed pre-prepare is never overtaken by the prepare or commit the leader
// sends right after it. Messages of a view older than the newest view seen on
// the line are discarded when their turn comes, because the replica has moved
// on and the receiver would drop them as stale anyway. Discarding them also
// frees the line immediately once the replica starts voting in the next view.
//
// When nothing is held, submissions with no delay are sent inline, so the
// common path costs one mutex acquisition.
type proposalSendLine struct {
	mu         sync.Mutex
	queue      []delayedConsensusSend
	busy       bool
	latestView uint64
	notify     chan struct{}
	stopChan   chan struct{}
	done       chan struct{}
	stopOnce   sync.Once
	onDrop     func(view uint64)
}

type delayedConsensusSend struct {
	view      uint64
	releaseAt time.Time
	send      func()
}

func newProposalSendLine(onDrop func(view uint64)) *proposalSendLine {
	l := &proposalSendLine{
		notify:   make(chan struct{}, 1),
		stopChan: make(chan struct{}),
		done:     make(chan struct{}),
		onDrop:   onDrop,
	}
	go l.run()
	return l
}

// submit sends the message now when the line is idle and no delay applies,
// otherwise queues it behind whatever is already held.
func (l *proposalSendLine) submit(view uint64, delay time.Duration, send func()) {
	l.mu.Lock()
	if view > l.latestView {
		l.latestView = view
	}
	if len(l.queue) == 0 && !l.busy && delay <= 0 {
		l.mu.Unlock()
		send()
		return
	}
	l.queue = append(l.queue, delayedConsensusSend{
		view:      view,
		releaseAt: time.Now().Add(delay),
		send:      send,
	})
	l.mu.Unlock()
	l.wake()
}

func (l *proposalSendLine) wake() {
	select {
	case l.notify <- struct{}{}:
	default:
	}
}

func (l *proposalSendLine) stop() {
	l.stopOnce.Do(func() { close(l.stopChan) })
	<-l.done
}

func (l *proposalSendLine) run() {
	defer close(l.done)
	for {
		l.mu.Lock()
		if len(l.queue) == 0 {
			l.mu.Unlock()
			select {
			case <-l.notify:
				continue
			case <-l.stopChan:
				return
			}
		}
		item := l.queue[0]
		l.queue = l.queue[1:]
		l.busy = true
		l.mu.Unlock()

		if !l.release(item) {
			return
		}

		l.mu.Lock()
		l.busy = false
		l.mu.Unlock()
	}
}

// release waits for the item's release time, then sends it unless a newer view
// has been seen meanwhile. It returns false when the line is stopped.
func (l *proposalSendLine) release(item delayedConsensusSend) bool {
	for {
		if l.isStale(item.view) {
			if l.onDrop != nil {
				l.onDrop(item.view)
			}
			return true
		}
		wait := time.Until(item.releaseAt)
		if wait <= 0 {
			item.send()
			return true
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-l.notify:
			// A newer submission may have advanced the view; re-check.
			timer.Stop()
		case <-l.stopChan:
			timer.Stop()
			return false
		}
	}
}

func (l *proposalSendLine) isStale(view uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return view < l.latestView
}

// consensusMessageView reports the view of the messages that belong to a
// proposal's ordering: pre-prepare, prepare, and commit. Every other message
// type bypasses the send line.
func consensusMessageView(m *smartbftprotos.Message) (uint64, bool) {
	switch content := m.GetContent().(type) {
	case *smartbftprotos.Message_PrePrepare:
		return content.PrePrepare.View, true
	case *smartbftprotos.Message_Prepare:
		return content.Prepare.View, true
	case *smartbftprotos.Message_Commit:
		return content.Commit.View, true
	default:
		return 0, false
	}
}
