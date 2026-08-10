// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"context"
	"fmt"
	"time"

	adaptivetimers "github.com/hyperledger-labs/SmartBFT/proto/adaptive_timers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const agentRPCTimeout = 5 * time.Second

// agentClient delivers agreed report batches to the learning agent that runs
// alongside this sharing server.
type agentClient struct {
	nodeID uint64
	conn   *grpc.ClientConn
	client adaptivetimers.LearningAgentClient
}

func newAgentClient(nodeID uint64, target string) (*agentClient, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to learning agent at %s: %w", target, err)
	}
	return &agentClient{
		nodeID: nodeID,
		conn:   conn,
		client: adaptivetimers.NewLearningAgentClient(conn),
	}, nil
}

func (c *agentClient) close() {
	if c == nil || c.conn == nil {
		return
	}
	_ = c.conn.Close()
}

// deliverConsensus is called from the report chain's delivery worker, never
// from the consensus thread itself.
func (c *agentClient) deliverConsensus(batch *adaptivetimers.ReportBatch) {
	if c == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), agentRPCTimeout)
	defer cancel()
	if _, err := c.client.DeliverConsensus(ctx, batch); err != nil {
		fmt.Printf("%s DeliverConsensus failed: node=%d episode=%d err=%v\n",
			logTag("sharing"), c.nodeID, batch.Episode, err)
		return
	}
	fmt.Printf("%s delivered to agent: node=%d episode=%d reports=%d\n",
		logTag("sharing"), c.nodeID, batch.Episode, len(batch.Reports))
}
