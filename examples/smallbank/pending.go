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

// pendingTracker holds per-request state only until the request is decided or
// completed, so its size is bounded by the requests in flight.
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

func (p *pendingTracker) markSubmitted(req request) {
	key := requestKey(req.ClientID, req.ID)

	p.lock.Lock()
	if _, exists := p.submitted[key]; !exists {
		p.submitted[key] = time.Now()
	}
	p.lock.Unlock()
}

func (p *pendingTracker) complete(resp response) {
	key := requestKey(resp.ClientID, resp.ID)

	p.lock.Lock()
	ch, exists := p.waiters[key]
	if exists {
		delete(p.waiters, key)
	}
	delete(p.submitted, key)
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

// latencyFor returns the time since req was first seen and forgets the entry;
// a request is decided at most once.
func (p *pendingTracker) latencyFor(req request, now time.Time) (time.Duration, bool) {
	key := requestKey(req.ClientID, req.ID)

	p.lock.Lock()
	defer p.lock.Unlock()

	submitted, exists := p.submitted[key]
	if !exists {
		return 0, false
	}
	delete(p.submitted, key)
	return now.Sub(submitted), true
}
