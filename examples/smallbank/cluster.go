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

type cluster struct {
	nodes   map[uint64]*node
	pending *pendingTracker
	logger  smart.Logger
}

func newCluster(numNodes int, opts nodeOptions, testDir string, verbose bool) (*cluster, error) {
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

	level := zap.NewAtomicLevelAt(zapcore.PanicLevel)
	if verbose {
		level.SetLevel(zapcore.InfoLevel)
	}
	zapLogger := zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
		zapcore.AddSync(zapcore.Lock(zapcore.AddSync(discardWriter{}))),
		level,
	))
	if verbose {
		zapLogger, _ = zap.NewDevelopment()
	}
	logger := zapLogger.Sugar()

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
		out := make(map[uint64]chan<- wireMessage)
		for targetID, ch := range channels {
			if targetID == uint64(id) {
				continue
			}
			out[targetID] = ch
		}

		nodeOpts := opts
		learning, err := newLearningManager(optsLearningForNode(opts, uint64(id)))
		if err != nil {
			c.stop()
			return nil, fmt.Errorf("create learning manager for node %d: %w", id, err)
		}
		nodeOpts.Learning = learning
		n, err := newNode(uint64(id), channels[uint64(id)], out, c.pending, logger, walMet, bftMet, nodeOpts, testDir)
		if err != nil {
			nodeOpts.Learning.close()
			c.stop()
			return nil, err
		}
		c.nodes[uint64(id)] = n
	}

	return c, nil
}

func optsLearningForNode(opts nodeOptions, nodeID uint64) learningOptions {
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
	if err := leader.consensus.SubmitRequest(raw); err != nil {
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
		raw := mustJSON(struct {
			Accounts map[uint64]string `json:"accounts"`
			Checking map[uint64]int64  `json:"checking"`
			Savings  map[uint64]int64  `json:"savings"`
		}{
			Accounts: n.state.accounts,
			Checking: n.state.checking,
			Savings:  n.state.savings,
		})
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
