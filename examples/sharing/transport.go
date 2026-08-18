// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger-labs/SmartBFT/examples/internal/fabrictransport"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"google.golang.org/protobuf/proto"
)

const (
	operationReportConsensus     = "report-consensus"
	operationReportTransaction   = "report-transaction"
	operationReportSubmit        = "report-submit"
	operationReportStateTransfer = "report-state-transfer"

	reportQueueSize   = 1024
	reportSendTimeout = 2 * time.Second
)

type reportAck struct{}

type reportConsensusRequest struct {
	From    uint64
	Message []byte
}

type reportTransactionRequest struct {
	From    uint64
	Payload []byte
}

// reportSubmitRequest carries a marshaled ReportBatch that a peer's learning
// agent handed to its local sharing server. Submissions are broadcast so the
// current leader learns about an episode without waiting for SmartBFT's
// leader-forwarding timeout.
type reportSubmitRequest struct {
	From    uint64
	Payload []byte
}

// latestDecisionRequest asks a peer for its tip of the report chain.
type latestDecisionRequest struct{}

// reportTransport uses one Fabric-style operation stream per peer and traffic
// class. Consensus overflow cancels that stream; transaction and submission
// traffic waits for queue space, exactly like Fabric's submit streams.
type reportTransport struct {
	selfID   uint64
	hosts    map[uint64]hostEntry
	ids      []uint64
	clients  map[uint64]*fabrictransport.Client
	stopOnce sync.Once
}

func newReportTransport(selfID uint64, hosts []hostEntry) (*reportTransport, error) {
	t := &reportTransport{
		selfID:  selfID,
		hosts:   make(map[uint64]hostEntry, len(hosts)),
		ids:     nodeIDs(hosts),
		clients: make(map[uint64]*fabrictransport.Client),
	}
	for _, host := range hosts {
		t.hosts[host.ID] = host
		if host.ID == selfID {
			continue
		}
		peerID := host.ID
		client, err := fabrictransport.NewClient(fabrictransport.ClientConfig{
			SelfID:      selfID,
			Address:     host.reportAddress(),
			QueueSize:   reportQueueSize,
			SendTimeout: reportSendTimeout,
			OnError: func(operation string, err error) {
				fmt.Printf("%s stream to sharing node %d failed: operation=%s err=%v\n",
					logTag("transport"), peerID, operation, err)
			},
		})
		if err != nil {
			t.close()
			return nil, fmt.Errorf("create transport to sharing node %d at %s: %w", host.ID, host.reportAddress(), err)
		}
		t.clients[host.ID] = client
	}
	return t, nil
}

func (t *reportTransport) close() {
	if t == nil {
		return
	}
	t.stopOnce.Do(func() {
		for id, client := range t.clients {
			if err := client.Close(); err != nil {
				fmt.Printf("%s closing stream client to sharing node %d failed: %v\n", logTag("transport"), id, err)
			}
		}
	})
}

func (t *reportTransport) nodeIDs() []uint64 {
	ids := make([]uint64, len(t.ids))
	copy(ids, t.ids)
	return ids
}

func (t *reportTransport) sendConsensus(targetID uint64, message *smartbftprotos.Message) {
	raw, err := proto.Marshal(message)
	if err != nil {
		fmt.Printf("%s marshal consensus message for sharing node %d failed: %v\n", logTag("transport"), targetID, err)
		return
	}
	t.send(targetID, operationReportConsensus, &reportConsensusRequest{Message: raw}, true)
}

func (t *reportTransport) sendTransaction(targetID uint64, payload []byte) {
	t.send(targetID, operationReportTransaction, &reportTransactionRequest{Payload: append([]byte(nil), payload...)}, false)
}

// broadcastSubmit hands a locally submitted batch to every peer so that
// whichever node currently leads the report chain can propose it immediately.
func (t *reportTransport) broadcastSubmit(payload []byte) {
	for _, id := range t.ids {
		if id == t.selfID {
			continue
		}
		t.send(id, operationReportSubmit, &reportSubmitRequest{Payload: append([]byte(nil), payload...)}, false)
	}
}

func (t *reportTransport) send(targetID uint64, operation string, request any, allowDrop bool) {
	client := t.clients[targetID]
	if client == nil {
		fmt.Printf("%s no route to sharing node %d: operation=%s\n", logTag("transport"), targetID, operation)
		return
	}
	payload, err := fabrictransport.Marshal(request)
	if err != nil {
		fmt.Printf("%s marshal message to sharing node %d failed: operation=%s err=%v\n",
			logTag("transport"), targetID, operation, err)
		return
	}
	if err := client.Send(operation, payload, allowDrop, nil); err != nil {
		fmt.Printf("%s enqueue message to sharing node %d failed: operation=%s err=%v\n",
			logTag("transport"), targetID, operation, err)
	}
}
