// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger-labs/SmartBFT/pkg/api"
	"github.com/hyperledger-labs/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	"golang.org/x/sync/semaphore"
)

const (
	defaultRequestTimeout    = 10 * time.Second // for unit tests only
	defaultMaxBytes          = 100 * 1024       // default max request size would be of size 100Kb
	defaultSizeOfDelElements = 1000             // default size slice of delete elements
	defaultEraseTimeout      = 5 * time.Second  // for cycle erase silice of delete elements
)

var (
	ErrReqAlreadyExists    = errors.New("request already exists")
	ErrReqAlreadyProcessed = errors.New("request already processed")
	ErrRequestTooBig       = errors.New("submitted request is too big")
	ErrSubmitTimeout       = errors.New("timeout submitting to request pool")
)

//go:generate mockery -dir . -name RequestTimeoutHandler -case underscore -output ./mocks/

// RequestTimeoutHandler defines the methods called by request timeout timers created by time.AfterFunc.
// This interface is implemented by the bft.Controller.
type RequestTimeoutHandler interface {
	// OnRequestTimeout is called when a request timeout expires.
	OnRequestTimeout(request []byte, requestInfo types.RequestInfo)
	// OnLeaderFwdRequestTimeout is called when a leader forwarding timeout expires.
	OnLeaderFwdRequestTimeout(request []byte, requestInfo types.RequestInfo)
	// OnAutoRemoveTimeout is called when a auto-remove timeout expires.
	OnAutoRemoveTimeout(requestInfo types.RequestInfo)
}

// Pool implements a requests pool, maintaining a pool of a given size provided during
// construction. If there are more incoming requests than the given size, it will
// block during submission until there is space to submit new ones.
type Pool struct {
	logger    api.Logger
	metrics   *api.MetricsRequestPool
	inspector api.RequestInspector
	options   PoolOptions

	cancel         context.CancelFunc
	lock           sync.RWMutex
	fifo           *list.List
	semaphore      *semaphore.Weighted
	existMap       map[types.RequestInfo]*list.Element
	timeoutHandler RequestTimeoutHandler
	closed         bool
	stopped        bool
	submittedChan  chan struct{}
	sizeBytes      uint64
	delMap         map[types.RequestInfo]struct{}
	delSlice       []types.RequestInfo

	// Only the oldest pending request is timed. Timing every request makes the
	// complaint threshold depend on queue depth instead of on progress: a deep
	// backlog would complain about a leader that is committing at full speed.
	// progress runs the forward -> complain chain for the head, and gc removes
	// the head once it is older than AutoRemoveTimeout. Both move on to the
	// next request when the head leaves the pool.
	progress headTimer
	gc       headTimer
}

// requestItem captures request related information
type requestItem struct {
	info              types.RequestInfo
	request           []byte
	additionTimestamp time.Time
}

// headTimer is a timer that follows the request at the front of the FIFO.
// gen invalidates callbacks that belong to an earlier arming.
type headTimer struct {
	timer  *time.Timer
	owner  types.RequestInfo
	active bool
	gen    uint64
}

func (t *headTimer) stop() {
	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
	t.active = false
	t.gen++
}

func (t *headTimer) tracks(info types.RequestInfo) bool {
	return t.active && t.owner == info
}

// PoolOptions is the pool configuration
type PoolOptions struct {
	QueueSize         int64
	ForwardTimeout    time.Duration
	ComplainTimeout   time.Duration
	AutoRemoveTimeout time.Duration
	RequestMaxBytes   uint64
	SubmitTimeout     time.Duration
	Metrics           *api.MetricsRequestPool
}

// NewPool constructs new requests pool
func NewPool(log api.Logger, inspector api.RequestInspector, th RequestTimeoutHandler, options PoolOptions, submittedChan chan struct{}) *Pool {
	if options.ForwardTimeout == 0 {
		options.ForwardTimeout = defaultRequestTimeout
	}
	if options.ComplainTimeout == 0 {
		options.ComplainTimeout = defaultRequestTimeout
	}
	if options.AutoRemoveTimeout == 0 {
		options.AutoRemoveTimeout = defaultRequestTimeout
	}
	if options.RequestMaxBytes == 0 {
		options.RequestMaxBytes = defaultMaxBytes
	}
	if options.SubmitTimeout == 0 {
		options.SubmitTimeout = defaultRequestTimeout
	}
	if options.Metrics == nil {
		options.Metrics = api.NewMetricsRequestPool(&disabled.Provider{})
	}

	ctx, cancel := context.WithCancel(context.Background())

	rp := &Pool{
		cancel:         cancel,
		timeoutHandler: th,
		logger:         log,
		metrics:        options.Metrics,
		inspector:      inspector,
		fifo:           list.New(),
		semaphore:      semaphore.NewWeighted(options.QueueSize),
		existMap:       make(map[types.RequestInfo]*list.Element),
		options:        options,
		submittedChan:  submittedChan,
		delMap:         make(map[types.RequestInfo]struct{}),
		delSlice:       make([]types.RequestInfo, 0, defaultSizeOfDelElements),
	}

	go func() {
		tic := time.NewTicker(defaultEraseTimeout)

		for {
			select {
			case <-tic.C:
				rp.eraseFromDelSlice()
			case <-ctx.Done():
				tic.Stop()

				return
			}
		}
	}()

	return rp
}

// ChangeOptions changes the options of the pool
func (rp *Pool) ChangeOptions(th RequestTimeoutHandler, options PoolOptions) {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	if !rp.stopped {
		rp.logger.Errorf("Trying to change timeouts but the pool is not stopped")
		return
	}

	if options.ForwardTimeout == 0 {
		options.ForwardTimeout = defaultRequestTimeout
	}
	if options.ComplainTimeout == 0 {
		options.ComplainTimeout = defaultRequestTimeout
	}
	if options.AutoRemoveTimeout == 0 {
		options.AutoRemoveTimeout = defaultRequestTimeout
	}
	if options.RequestMaxBytes == 0 {
		options.RequestMaxBytes = defaultMaxBytes
	}
	if options.SubmitTimeout == 0 {
		options.SubmitTimeout = defaultRequestTimeout
	}

	rp.options.ForwardTimeout = options.ForwardTimeout
	rp.options.ComplainTimeout = options.ComplainTimeout
	rp.options.AutoRemoveTimeout = options.AutoRemoveTimeout
	rp.options.RequestMaxBytes = options.RequestMaxBytes
	rp.options.SubmitTimeout = options.SubmitTimeout

	rp.timeoutHandler = th

	rp.logger.Debugf("Changed pool timeouts")
}

func (rp *Pool) isClosed() bool {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	return rp.closed
}

// Submit a request into the pool, returns an error when request is already in the pool
func (rp *Pool) Submit(request []byte) error {
	reqInfo := rp.inspector.RequestID(request)
	if rp.isClosed() {
		return fmt.Errorf("pool closed, request rejected: %s", reqInfo)
	}

	if uint64(len(request)) > rp.options.RequestMaxBytes {
		rp.metrics.CountOfFailAddRequestToPool.With(
			rp.metrics.LabelsForWith("reason", api.ReasonRequestMaxBytes)...,
		).Add(1)
		return fmt.Errorf(
			"submitted request (%d) is bigger than request max bytes (%d)",
			len(request),
			rp.options.RequestMaxBytes,
		)
	}

	rp.lock.RLock()
	_, alreadyExists := rp.existMap[reqInfo]
	_, alreadyDelete := rp.delMap[reqInfo]
	rp.lock.RUnlock()

	if alreadyExists {
		rp.logger.Debugf("request %s already exists in the pool", reqInfo)
		return ErrReqAlreadyExists
	}

	if alreadyDelete {
		rp.logger.Debugf("request %s already processed", reqInfo)
		return ErrReqAlreadyProcessed
	}

	ctx, cancel := context.WithTimeout(context.Background(), rp.options.SubmitTimeout)
	defer cancel()
	// Do not wait for a semaphore with a lock, as it will prevent draining the pool.
	if err := rp.semaphore.Acquire(ctx, 1); err != nil {
		rp.metrics.CountOfFailAddRequestToPool.With(
			rp.metrics.LabelsForWith("reason", api.ReasonSemaphoreAcquireFail)...,
		).Add(1)
		return fmt.Errorf("acquiring semaphore for request: %s: %w", reqInfo, err)
	}

	reqCopy := append(make([]byte, 0), request...)

	rp.lock.Lock()
	defer rp.lock.Unlock()

	if _, existsEl := rp.existMap[reqInfo]; existsEl {
		rp.semaphore.Release(1)
		rp.logger.Debugf("request %s has been already added to the pool", reqInfo)
		return ErrReqAlreadyExists
	}

	if _, deleteEl := rp.delMap[reqInfo]; deleteEl {
		rp.semaphore.Release(1)
		rp.logger.Debugf("request %s has been already processed", reqInfo)
		return ErrReqAlreadyProcessed
	}

	reqItem := &requestItem{
		info:              reqInfo,
		request:           reqCopy,
		additionTimestamp: time.Now(),
	}

	element := rp.fifo.PushBack(reqItem)
	rp.metrics.CountOfRequestPool.Set(float64(rp.fifo.Len()))
	rp.metrics.CountOfRequestPoolAll.Add(1)
	rp.existMap[reqInfo] = element

	if len(rp.existMap) != rp.fifo.Len() {
		rp.logger.Panicf("RequestPool map and list are of different length: map=%d, list=%d", len(rp.existMap), rp.fifo.Len())
	}

	if rp.stopped {
		rp.logger.Debugf("pool stopped, submitting without a timer, request: %s", reqInfo)
	}
	rp.armTimers()
	rp.logger.Debugf("Request %s submitted", reqInfo)

	// notify that a request was submitted
	select {
	case rp.submittedChan <- struct{}{}:
	default:
	}

	rp.sizeBytes += uint64(len(element.Value.(*requestItem).request))

	return nil
}

// Size returns the number of requests currently residing the pool
func (rp *Pool) Size() int {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	return len(rp.existMap)
}

// NextRequests returns the next requests to be batched.
// It returns at most maxCount requests, and at most maxSizeBytes, in a newly allocated slice.
// Return variable full indicates that the batch cannot be increased further by calling again with the same arguments.
func (rp *Pool) NextRequests(maxCount int, maxSizeBytes uint64, check bool) (batch [][]byte, full bool) {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	if check {
		if (len(rp.existMap) < maxCount) && (rp.sizeBytes < maxSizeBytes) {
			return nil, false
		}
	}

	count := min(rp.fifo.Len(), maxCount)
	var totalSize uint64
	batch = make([][]byte, 0, count)
	element := rp.fifo.Front()
	for range count {
		req := element.Value.(*requestItem).request
		reqLen := uint64(len(req))
		if totalSize+reqLen > maxSizeBytes {
			rp.logger.Debugf("Returning batch of %d requests totalling %dB as it exceeds threshold of %dB",
				len(batch), totalSize, maxSizeBytes)
			return batch, true
		}
		batch = append(batch, req)
		totalSize += reqLen
		element = element.Next()
	}

	fullS := totalSize >= maxSizeBytes
	fullC := len(batch) == maxCount
	full = fullS || fullC
	if len(batch) > 0 {
		rp.logger.Debugf("Returning batch of %d requests totalling %dB",
			len(batch), totalSize)
	}
	return batch, full
}

// Prune removes requests for which the given predicate returns error.
func (rp *Pool) Prune(predicate func([]byte) error) {
	reqVec, infoVec := rp.copyRequests()

	var numPruned int
	for i, req := range reqVec {
		err := predicate(req)
		if err == nil {
			continue
		}

		if remErr := rp.RemoveRequest(infoVec[i]); remErr != nil {
			rp.logger.Debugf("Failed to prune request: %s; predicate error: %s; remove error: %s", infoVec[i], err, remErr)
		} else {
			rp.logger.Debugf("Pruned request: %s; predicate error: %s", infoVec[i], err)
			numPruned++
		}
	}

	rp.logger.Debugf("Pruned %d requests", numPruned)
}

func (rp *Pool) copyRequests() (requestVec [][]byte, infoVec []types.RequestInfo) {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	requestVec = make([][]byte, len(rp.existMap))
	infoVec = make([]types.RequestInfo, len(rp.existMap))

	var i int
	for info, item := range rp.existMap {
		infoVec[i] = info
		requestVec[i] = item.Value.(*requestItem).request
		i++
	}

	return
}

// RemoveRequest removes the given request from the pool.
func (rp *Pool) RemoveRequest(requestInfo types.RequestInfo) error {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	if !rp.removeLocked(requestInfo) {
		errStr := fmt.Sprintf("request %s is not in the pool at remove time", requestInfo)
		rp.logger.Debugf(errStr)
		return errors.New(errStr)
	}
	return nil
}

// removeLocked removes the request if it is in the pool. Called with the lock held.
func (rp *Pool) removeLocked(requestInfo types.RequestInfo) bool {
	element, exist := rp.existMap[requestInfo]
	if !exist {
		rp.moveToDelSlice(requestInfo)
		return false
	}

	rp.deleteRequest(element, requestInfo)
	rp.sizeBytes -= uint64(len(element.Value.(*requestItem).request))
	return true
}

func (rp *Pool) deleteRequest(element *list.Element, requestInfo types.RequestInfo) {
	item := element.Value.(*requestItem)
	wasHead := rp.fifo.Front() == element

	rp.fifo.Remove(element)
	rp.metrics.CountOfRequestPool.Set(float64(rp.fifo.Len()))
	rp.metrics.LatencyOfRequestPool.Observe(time.Since(item.additionTimestamp).Seconds())
	delete(rp.existMap, requestInfo)
	rp.moveToDelSlice(requestInfo)
	rp.logger.Debugf("Removed request %s from request pool", requestInfo)
	rp.semaphore.Release(1)

	if len(rp.existMap) != rp.fifo.Len() {
		rp.logger.Panicf("RequestPool map and list are of different length: map=%d, list=%d", len(rp.existMap), rp.fifo.Len())
	}

	if wasHead {
		rp.armTimers()
	}
}

func (rp *Pool) moveToDelSlice(requestInfo types.RequestInfo) {
	_, exist := rp.delMap[requestInfo]
	if exist {
		return
	}

	rp.delMap[requestInfo] = struct{}{}
	rp.delSlice = append(rp.delSlice, requestInfo)
}

func (rp *Pool) eraseFromDelSlice() {
	rp.lock.RLock()
	l := len(rp.delSlice)
	rp.lock.RUnlock()

	if l <= defaultSizeOfDelElements {
		return
	}

	rp.lock.Lock()
	defer rp.lock.Unlock()

	n := len(rp.delSlice) - defaultSizeOfDelElements

	for _, r := range rp.delSlice[:n] {
		delete(rp.delMap, r)
	}

	rp.delSlice = rp.delSlice[n:]
}

// armTimers points the head timers at the request at the front of the FIFO.
// A timer already tracking that request is left alone, so the head keeps its
// budget when other requests are added or removed. Called with the lock held.
func (rp *Pool) armTimers() {
	if rp.closed || rp.stopped {
		return
	}

	front := rp.fifo.Front()
	if front == nil {
		rp.progress.stop()
		rp.gc.stop()
		return
	}

	item := front.Value.(*requestItem)
	if !rp.progress.tracks(item.info) {
		rp.startProgress(item)
	}
	if !rp.gc.tracks(item.info) {
		rp.startGC(item)
	}
}

// startProgress gives the head a fresh RequestForwardTimeout. Called with the lock held.
func (rp *Pool) startProgress(item *requestItem) {
	rp.progress.stop()
	rp.progress.owner = item.info
	rp.progress.active = true

	gen := rp.progress.gen
	request, reqInfo := item.request, item.info
	rp.progress.timer = time.AfterFunc(
		rp.options.ForwardTimeout,
		func() { rp.onRequestTO(gen, request, reqInfo) },
	)
	rp.logger.Debugf("Request %s is at the head of the pool; started a timeout: %s", reqInfo, rp.options.ForwardTimeout)
}

// startGC arms removal of the head at its age limit. The limit is measured
// from submission, so stopping and restarting the timers never extends a
// request's lifetime, and a head that is already overdue is removed at once.
// Called with the lock held.
func (rp *Pool) startGC(item *requestItem) {
	rp.gc.stop()
	rp.gc.owner = item.info
	rp.gc.active = true

	gen := rp.gc.gen
	reqInfo := item.info
	delay := time.Until(item.additionTimestamp.Add(rp.options.AutoRemoveTimeout))
	if delay < 0 {
		delay = 0
	}
	rp.gc.timer = time.AfterFunc(delay, func() { rp.onAutoRemoveTO(gen, reqInfo) })
}

// Close removes all the requests, stops all the timeout timers.
func (rp *Pool) Close() {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	rp.closed = true
	rp.progress.stop()
	rp.gc.stop()

	for requestInfo, element := range rp.existMap {
		rp.deleteRequest(element, requestInfo)
	}

	rp.cancel()
}

// StopTimers stops the head-of-line timers and marks the pool as "stopped", which keeps the head
// untimed until RestartTimers, including by timer go-routines that were running at the time of
// the call to StopTimers().
func (rp *Pool) StopTimers() {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	rp.stopped = true
	rp.progress.stop()
	rp.gc.stop()

	rp.logger.Debugf("Stopped timers: size=%d", len(rp.existMap))
}

// RestartTimers re-allows timing and gives the request at the head of the pool a fresh
// RequestForwardTimeout budget. The auto-remove age limit is not extended.
func (rp *Pool) RestartTimers() {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	rp.stopped = false
	rp.progress.stop()
	rp.gc.stop()
	rp.armTimers()

	rp.logger.Debugf("Restarted timers: size=%d", len(rp.existMap))
}

// called by the goroutine spawned by time.AfterFunc
func (rp *Pool) onRequestTO(gen uint64, request []byte, reqInfo types.RequestInfo) {
	rp.lock.Lock()

	// Any change of head, stop, or close re-arms or stops the timer and bumps
	// its generation, so a matching generation means the request is still the
	// untouched head of a running pool.
	if gen != rp.progress.gen {
		rp.lock.Unlock()
		rp.logger.Debugf("Request %s is no longer the timed head of the pool", reqInfo)
		return
	}

	// start a second timeout
	rp.progress.timer = time.AfterFunc(
		rp.options.ComplainTimeout,
		func() { rp.onLeaderFwdRequestTO(gen, request, reqInfo) },
	)
	rp.logger.Debugf("Request %s; started a leader-forwarding timeout: %s", reqInfo, rp.options.ComplainTimeout)

	rp.lock.Unlock()

	// may take time, in case Comm channel to leader is full; hence w/o the lock.
	rp.logger.Debugf("Request %s timeout expired, going to send to leader", reqInfo)
	rp.metrics.CountOfLeaderForwardRequest.Add(1)
	rp.timeoutHandler.OnRequestTimeout(request, reqInfo)
}

// called by the goroutine spawned by time.AfterFunc
func (rp *Pool) onLeaderFwdRequestTO(gen uint64, request []byte, reqInfo types.RequestInfo) {
	rp.lock.Lock()

	if gen != rp.progress.gen {
		rp.lock.Unlock()
		rp.logger.Debugf("Request %s is no longer the timed head of the pool", reqInfo)
		return
	}

	// The chain ends here. The head stays until it is delivered, the timers
	// are restarted, or it reaches its auto-remove age.
	rp.progress.timer = nil

	rp.lock.Unlock()

	// may take time, in case Comm channel is full; hence w/o the lock.
	rp.logger.Debugf("Request %s leader-forwarding timeout expired, going to complain on leader", reqInfo)
	rp.metrics.CountTimeoutTwoStep.Add(1)
	rp.timeoutHandler.OnLeaderFwdRequestTimeout(request, reqInfo)
}

// called by the goroutine spawned by time.AfterFunc
func (rp *Pool) onAutoRemoveTO(gen uint64, reqInfo types.RequestInfo) {
	rp.lock.Lock()

	if gen != rp.gc.gen {
		rp.lock.Unlock()
		rp.logger.Debugf("Request %s is no longer the head of the pool", reqInfo)
		return
	}

	rp.logger.Debugf("Request %s auto-remove timeout expired, going to remove from pool", reqInfo)
	removed := rp.removeLocked(reqInfo)

	rp.lock.Unlock()

	if !removed {
		rp.logger.Errorf("Removal of request %s failed; it is not in the pool", reqInfo)
		return
	}
	rp.metrics.CountOfDeleteRequestPool.Add(1)
	rp.timeoutHandler.OnAutoRemoveTimeout(reqInfo)
}
