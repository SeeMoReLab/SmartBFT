// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"sync"
	"time"
)

// pendingTracker records when each request was first seen, for the consensus
// latency reported to the learning agent. An entry lives only until its
// request is decided or completed, so the map is bounded by requests in flight.
type pendingTracker struct {
	lock      sync.Mutex
	submitted map[string]time.Time
}

func newPendingTracker() *pendingTracker {
	return &pendingTracker{submitted: make(map[string]time.Time)}
}

func (p *pendingTracker) markSubmitted(req request) {
	key := requestKey(req.ClientID, req.ID)

	p.lock.Lock()
	if _, exists := p.submitted[key]; !exists {
		p.submitted[key] = time.Now()
	}
	p.lock.Unlock()
}

// complete forgets a request that finished without being decided again, for
// example a duplicate of one already executed.
func (p *pendingTracker) complete(resp response) {
	key := requestKey(resp.ClientID, resp.ID)

	p.lock.Lock()
	delete(p.submitted, key)
	p.lock.Unlock()
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
