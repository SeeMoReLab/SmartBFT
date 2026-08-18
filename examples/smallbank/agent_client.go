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

type learningAgentClient struct {
	conn   *grpc.ClientConn
	client adaptivetimers.LearningAgentClient
}

func newLearningAgentClient(target string, timeout time.Duration) (*learningAgentClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	return &learningAgentClient{
		conn:   conn,
		client: adaptivetimers.NewLearningAgentClient(conn),
	}, nil
}

func (c *learningAgentClient) close() {
	if c == nil || c.conn == nil {
		return
	}
	_ = c.conn.Close()
}

func (c *learningAgentClient) sendReport(ctx context.Context, report *adaptivetimers.ReportLocal) error {
	if c == nil {
		return fmt.Errorf("learning agent client is nil")
	}
	_, err := c.client.SendReport(ctx, report)
	return err
}

func (c *learningAgentClient) getTimeout(ctx context.Context, episode uint32) (*adaptivetimers.TimeoutStatus, error) {
	if c == nil {
		return nil, fmt.Errorf("learning agent client is nil")
	}
	return c.client.GetTimeout(ctx, &adaptivetimers.TimeoutRequest{
		Episode:  episode,
		Protocol: adaptivetimers.Protocol_PROTOCOL_PBFT,
	})
}
