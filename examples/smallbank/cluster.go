// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"context"
	"fmt"
	"time"

	smart "github.com/hyperledger-labs/SmartBFT/pkg/api"
	"github.com/hyperledger-labs/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger-labs/SmartBFT/pkg/wal"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type smallBankLogMode int

const (
	smallBankLogModeQuiet smallBankLogMode = iota
	smallBankLogModeDebug
)

type cluster struct {
	nodes   map[uint64]*node
	pending *pendingTracker
	logger  smart.Logger
}

func newCluster(numNodes int, opts nodeOptions, testDir string, logMode smallBankLogMode) (*cluster, error) {
	if numNodes < 4 {
		return nil, fmt.Errorf("SmartBFT requires at least 4 nodes for this example")
	}
	if opts.Failures == nil {
		opts.Failures = disabledProposalDelayController()
	}
	if opts.Learning != nil {
		return nil, fmt.Errorf("nodeOptions.Learning must be constructed per node")
	}
	if opts.NumNodes == 0 {
		opts.NumNodes = numNodes
	}

	logger := newSmallBankLogger(logMode)

	channels := make(map[uint64]chan wireMessage)
	for id := 1; id <= numNodes; id++ {
		channels[uint64(id)] = make(chan wireMessage, 4096)
	}

	c := &cluster{
		nodes:   make(map[uint64]*node),
		pending: newPendingTracker(),
		logger:  logger,
	}
	met := &disabled.Provider{}
	walMet := wal.NewMetrics(met, "smallbank")
	bftMet := smart.NewMetrics(met, "smallbank")

	for id := 1; id <= numNodes; id++ {
		nodeID := uint64(id)
		out := make(map[uint64]chan<- wireMessage)
		for targetID, ch := range channels {
			if targetID == nodeID {
				continue
			}
			out[targetID] = ch
		}

		nodeOpts := opts
		var localNode *node
		learningOpts := c.optsLearningForNode(opts, nodeID)
		if learningOpts.Enabled {
			learningOpts.ApplyTimeout = func(timeout time.Duration) error {
				if localNode == nil {
					return fmt.Errorf("node %d not initialized", nodeID)
				}
				_, err := localNode.applyBaseRequestTimeout(timeout, "learning-recommendation")
				return err
			}
		}
		learning, err := newLearningManager(learningOpts)
		if err != nil {
			c.stop()
			return nil, fmt.Errorf("create learning manager for node %d: %w", nodeID, err)
		}
		nodeOpts.Learning = learning
		n, err := newNode(nodeID, channels[nodeID], out, c.pending, logger, walMet, bftMet, nodeOpts, testDir)
		if err != nil {
			nodeOpts.Learning.close()
			c.stop()
			return nil, err
		}
		localNode = n
		c.nodes[nodeID] = n
	}

	return c, nil
}

func (c *cluster) optsLearningForNode(opts nodeOptions, nodeID uint64) learningOptions {
	learning := opts.LearningOptions
	if !learning.Enabled {
		return learning
	}
	if learning.NodeID == 0 {
		learning.NodeID = 1
	}
	if learning.NodeID != nodeID {
		return learningOptions{}
	}
	learning.NodeID = nodeID
	return learning
}

func (c *cluster) stop() {
	for _, n := range c.nodes {
		n.stop()
	}
}

func (c *cluster) invoke(ctx context.Context, req request) (response, error) {
	raw, err := encodeRequest(req)
	if err != nil {
		return response{}, err
	}

	respCh, cancel := c.pending.register(req)
	defer cancel()

	leader := c.leader()
	if leader == nil {
		err := fmt.Errorf("no leader")
		c.pending.fail(req, err)
		return response{}, err
	}
	if err := leader.submitRequest(req.ClientID, req.ID, raw); err != nil {
		c.pending.fail(req, err)
		return response{}, err
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-ctx.Done():
		return response{}, ctx.Err()
	}
}

func (c *cluster) leader() *node {
	for _, n := range c.nodes {
		leaderID := n.consensus.GetLeaderID()
		if leaderID == 0 {
			continue
		}
		if leader, exists := c.nodes[leaderID]; exists {
			return leader
		}
	}
	return c.nodes[1]
}

func (c *cluster) waitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.leader() != nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for leader")
}

func (c *cluster) stateChecksums() map[uint64]string {
	checksums := make(map[uint64]string, len(c.nodes))
	for id, n := range c.nodes {
		n.stateLock.Lock()
		raw := mustJSON(n.state.deterministicStateSnapshot())
		n.stateLock.Unlock()
		checksums[id] = hashBytes(raw)
	}
	return checksums
}

func (c *cluster) waitForMatchingChecksums(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if checksumsMatch(c.stateChecksums()) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func checksumsMatch(checksums map[uint64]string) bool {
	var first string
	for _, checksum := range checksums {
		if first == "" {
			first = checksum
			continue
		}
		if checksum != first {
			return false
		}
	}
	return true
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func newSmallBankLogger(logMode smallBankLogMode) smart.Logger {
	if logMode == smallBankLogModeDebug {
		zapLogger, _ := zap.NewDevelopment()
		return zapLogger.Sugar()
	}

	level := zap.NewAtomicLevelAt(zapcore.PanicLevel)
	writer := zapcore.AddSync(discardWriter{})
	zapLogger := zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
		zapcore.AddSync(zapcore.Lock(writer)),
		level,
	))
	return zapLogger.Sugar()
}
