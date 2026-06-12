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
}

func newPendingTracker() *pendingTracker {
	return &pendingTracker{
		waiters:   make(map[string]chan response),
		submitted: make(map[string]time.Time),
	}
}

func (p *pendingTracker) register(req request) (<-chan response, func()) {
	key := requestKey(req.ClientID, req.ID)
	ch := make(chan response, 1)

	p.lock.Lock()
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

func (p *pendingTracker) complete(resp response) {
	key := requestKey(resp.ClientID, resp.ID)

	p.lock.Lock()
	ch, exists := p.waiters[key]
	if exists {
		delete(p.waiters, key)
	}
	p.lock.Unlock()

	if exists {
		ch <- resp
		close(ch)
	}
}

func (p *pendingTracker) fail(req request, err error) {
	p.complete(response{
		ClientID: req.ClientID,
		ID:       req.ID,
		Status:   statusSystemError,
		Error:    fmt.Sprintf("submit failed: %v", err),
	})
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
