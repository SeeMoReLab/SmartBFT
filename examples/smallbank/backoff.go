// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"fmt"
	"math"
	"sync"
	"time"
)

type requestTimeoutBackoffOptions struct {
	Enabled    bool
	MaxTimeout time.Duration
}

type requestTimeoutBackoffState struct {
	Enabled                           bool
	BaseTimeout                       time.Duration
	EffectiveTimeout                  time.Duration
	EffectiveViewChangeTimeout        time.Duration
	EffectiveViewChangeResendInterval time.Duration
	Multiplier                        int
	MaxTimeout                        time.Duration
}

type requestTimeoutBackoffUpdate struct {
	State    requestTimeoutBackoffState
	Previous requestTimeoutBackoffState
	Apply    bool
	Log      bool
	Decayed  bool
}

type requestTimeoutBackoff struct {
	lock sync.Mutex

	enabled                bool
	baseTimeout            time.Duration
	effectiveTimeout       time.Duration
	multiplier             int
	maxTimeout             time.Duration
	noProgressStreak       int
	haveNoProgressView     bool
	lastNoProgressView     uint64
	haveRequestTimeoutView bool
	lastRequestTimeoutView uint64
}

const maxViewChangeResendInterval = time.Second

func newRequestTimeoutBackoff(baseTimeout time.Duration, opts requestTimeoutBackoffOptions) (*requestTimeoutBackoff, error) {
	if baseTimeout <= 0 {
		return nil, fmt.Errorf("request timeout backoff base must be positive: %s", baseTimeout)
	}
	if opts.Enabled && opts.MaxTimeout <= 0 {
		return nil, fmt.Errorf("request timeout backoff max must be positive: %s", opts.MaxTimeout)
	}

	b := &requestTimeoutBackoff{
		enabled:     opts.Enabled,
		baseTimeout: baseTimeout,
		multiplier:  1,
		maxTimeout:  opts.MaxTimeout,
	}
	if !b.enabled {
		b.effectiveTimeout = baseTimeout
		return b, nil
	}
	b.clampMultiplierLocked()
	b.effectiveTimeout = b.computeEffectiveLocked()
	return b, nil
}

func (b *requestTimeoutBackoff) state() requestTimeoutBackoffState {
	if b == nil {
		return requestTimeoutBackoffState{}
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.stateLocked()
}

func (b *requestTimeoutBackoff) setBaseTimeout(timeout time.Duration) (requestTimeoutBackoffUpdate, error) {
	if b == nil || !b.enabled {
		return requestTimeoutBackoffUpdate{}, nil
	}
	if timeout <= 0 {
		return requestTimeoutBackoffUpdate{}, fmt.Errorf("request timeout backoff base must be positive: %s", timeout)
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	oldEffective := b.effectiveTimeout
	if oldEffective <= 0 {
		oldEffective = b.computeEffectiveLocked()
	}
	b.baseTimeout = timeout
	b.multiplier = int(math.Round(float64(oldEffective) / float64(timeout)))
	if b.multiplier < 1 {
		b.multiplier = 1
	}
	b.clampMultiplierLocked()
	b.effectiveTimeout = b.computeEffectiveLocked()
	return requestTimeoutBackoffUpdate{
		State: b.stateLocked(),
		Apply: true,
		Log:   true,
	}, nil
}

func (b *requestTimeoutBackoff) onRequestTimeout(view uint64) requestTimeoutBackoffUpdate {
	if b == nil || !b.enabled {
		return requestTimeoutBackoffUpdate{}
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	if b.haveRequestTimeoutView && view == b.lastRequestTimeoutView {
		return requestTimeoutBackoffUpdate{State: b.stateLocked()}
	}
	b.haveRequestTimeoutView = true
	b.lastRequestTimeoutView = view

	return requestTimeoutBackoffUpdate{
		State: b.stateLocked(),
		Log:   true,
	}
}

func (b *requestTimeoutBackoff) onNoProgressViewChange(targetView uint64) requestTimeoutBackoffUpdate {
	if b == nil || !b.enabled {
		return requestTimeoutBackoffUpdate{}
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	steps := 1
	if b.haveNoProgressView {
		if targetView <= b.lastNoProgressView {
			return requestTimeoutBackoffUpdate{State: b.stateLocked()}
		}
		steps = int(targetView - b.lastNoProgressView)
	} else if targetView > 0 {
		steps = int(targetView)
	}
	b.haveNoProgressView = true
	b.lastNoProgressView = targetView

	for i := 0; i < steps; i++ {
		if b.noProgressStreak > 0 {
			b.multiplier *= 2
		} else {
			b.noProgressStreak = 1
		}
		b.clampMultiplierLocked()
	}

	nextEffective := b.computeEffectiveLocked()
	changed := nextEffective != b.effectiveTimeout
	b.effectiveTimeout = nextEffective
	return requestTimeoutBackoffUpdate{
		State: b.stateLocked(),
		Apply: changed,
		Log:   true,
	}
}

func (b *requestTimeoutBackoff) onCommit(view uint64, sequence uint64) requestTimeoutBackoffUpdate {
	if b == nil || !b.enabled {
		return requestTimeoutBackoffUpdate{}
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	b.noProgressStreak = 0
	if !b.haveNoProgressView || view > b.lastNoProgressView {
		b.haveNoProgressView = true
		b.lastNoProgressView = view
	}
	b.haveRequestTimeoutView = false
	b.lastRequestTimeoutView = 0
	if b.multiplier <= 1 {
		return requestTimeoutBackoffUpdate{State: b.stateLocked()}
	}
	previous := b.stateLocked()
	b.multiplier--
	b.clampMultiplierLocked()
	nextEffective := b.computeEffectiveLocked()
	changed := nextEffective != b.effectiveTimeout
	b.effectiveTimeout = nextEffective
	return requestTimeoutBackoffUpdate{
		State:    b.stateLocked(),
		Previous: previous,
		Apply:    changed,
		Log:      changed,
		Decayed:  changed,
	}
}

func (b *requestTimeoutBackoff) stateLocked() requestTimeoutBackoffState {
	return requestTimeoutBackoffState{
		Enabled:                           b.enabled,
		BaseTimeout:                       b.baseTimeout,
		EffectiveTimeout:                  b.effectiveTimeout,
		EffectiveViewChangeTimeout:        b.computeEffectiveLocked(),
		EffectiveViewChangeResendInterval: b.computeViewChangeResendIntervalLocked(),
		Multiplier:                        b.multiplier,
		MaxTimeout:                        b.maxTimeout,
	}
}

func (b *requestTimeoutBackoff) clampMultiplierLocked() {
	if b.multiplier < 1 {
		b.multiplier = 1
	}
	if b.baseTimeout <= 0 || b.maxTimeout <= 0 {
		return
	}
	maxMultiplier := int(b.maxTimeout / b.baseTimeout)
	if maxMultiplier < 1 {
		maxMultiplier = 1
	}
	if b.multiplier > maxMultiplier {
		b.multiplier = maxMultiplier
	}
}

func (b *requestTimeoutBackoff) computeEffectiveLocked() time.Duration {
	effective := b.baseTimeout * time.Duration(b.multiplier)
	if b.maxTimeout > 0 && effective > b.maxTimeout {
		return b.maxTimeout
	}
	return effective
}

func (b *requestTimeoutBackoff) computeViewChangeResendIntervalLocked() time.Duration {
	effective := b.computeEffectiveLocked()
	cap := maxViewChangeResendInterval
	if b.baseTimeout > cap {
		cap = b.baseTimeout
	}
	if effective > cap {
		return cap
	}
	if effective < b.baseTimeout {
		return b.baseTimeout
	}
	return effective
}
