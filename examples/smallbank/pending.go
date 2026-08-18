// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"fmt"
	"sync"
	"time"
)

type pendingTracker struct {
	lock      sync.Mutex
	waiters   map[string]chan response
	submitted map[string]time.Time
	completed map[string]response
}

func newPendingTracker() *pendingTracker {
	return &pendingTracker{
		waiters:   make(map[string]chan response),
		submitted: make(map[string]time.Time),
		completed: make(map[string]response),
	}
}

func (p *pendingTracker) register(req request) (<-chan response, func()) {
	key := requestKey(req.ClientID, req.ID)
	ch := make(chan response, 1)

	p.lock.Lock()
	if resp, exists := p.completed[key]; exists {
		ch <- resp
		close(ch)
		p.lock.Unlock()
		return ch, func() {}
	}
	p.waiters[key] = ch
	p.submitted[key] = time.Now()
	p.lock.Unlock()

	cancel := func() {
		p.lock.Lock()
		delete(p.waiters, key)
		p.lock.Unlock()
	}

	return ch, cancel
}

func (p *pendingTracker) markSubmitted(req request) {
	key := requestKey(req.ClientID, req.ID)

	p.lock.Lock()
	if _, exists := p.submitted[key]; !exists {
		p.submitted[key] = time.Now()
	}
	p.lock.Unlock()
}

func (p *pendingTracker) complete(resp response) {
	p.completeInternal(resp, true)
}

func (p *pendingTracker) completeInternal(resp response, cache bool) {
	key := requestKey(resp.ClientID, resp.ID)

	p.lock.Lock()
	ch, exists := p.waiters[key]
	if exists {
		delete(p.waiters, key)
	}
	if cache {
		p.completed[key] = resp
	}
	p.lock.Unlock()

	if exists {
		ch <- resp
		close(ch)
	}
}

func (p *pendingTracker) fail(req request, err error) {
	p.completeInternal(response{
		ClientID: req.ClientID,
		ID:       req.ID,
		Status:   statusSystemError,
		Error:    fmt.Sprintf("submit failed: %v", err),
	}, false)
}

func (p *pendingTracker) latencyFor(req request, now time.Time) (time.Duration, bool) {
	key := requestKey(req.ClientID, req.ID)

	p.lock.Lock()
	defer p.lock.Unlock()

	submitted, exists := p.submitted[key]
	if !exists {
		return 0, false
	}
	return now.Sub(submitted), true
}
