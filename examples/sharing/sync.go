// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/hyperledger-labs/SmartBFT/examples/internal/fabrictransport"
	bft "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"google.golang.org/protobuf/proto"
)

// latestDecisionResponse is what a peer reports about its own tip of the report
// chain. Missing intermediate decisions are not fetched: a skipped episode only
// costs one learning update, so catching up to the tip is enough.
type latestDecisionResponse struct {
	NodeID   uint64
	HaveTip  bool
	View     uint64
	Sequence uint64
	Decision bft.Decision
}

type syncCandidate struct {
	response latestDecisionResponse
	votes    int
}

func faultTolerance(numNodes int) int {
	return (numNodes - 1) / 3
}

func decisionViewSequence(decision bft.Decision) (view uint64, sequence uint64, ok bool) {
	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(decision.Proposal.Metadata, md); err != nil {
		return 0, 0, false
	}
	return md.GetViewId(), md.GetLatestSequence(), true
}

// Sync brings this replica up to the report chain's tip. It trusts a tip only
// when f+1 nodes report it, which guarantees at least one honest witness.
func (n *reportNode) Sync() bft.SyncResponse {
	local, haveLocal := n.latestDecision()
	localSeq := uint64(0)
	if haveLocal {
		if _, seq, ok := decisionViewSequence(local); ok {
			localSeq = seq
		}
	}

	responses := n.transport.fetchLatestDecisions()
	best, found := selectSyncTarget(responses, faultTolerance(n.numNodes)+1)
	if !found || best.Sequence <= localSeq {
		fmt.Printf("%s sync: node=%d staying at seq=%d peers_returned=%d\n",
			logTag("sharing"), n.id, localSeq, len(responses))
		return bft.SyncResponse{
			Latest:   local,
			Reconfig: bft.ReconfigSync{InReplicatedDecisions: false},
		}
	}

	fmt.Printf("%s sync: node=%d advancing seq=%d -> seq=%d view=%d\n",
		logTag("sharing"), n.id, localSeq, best.Sequence, best.View)
	n.Deliver(best.Decision.Proposal, best.Decision.Signatures)

	return bft.SyncResponse{
		Latest:   best.Decision,
		Reconfig: bft.ReconfigSync{InReplicatedDecisions: false},
	}
}

// selectSyncTarget picks the highest sequence backed by at least required
// identical responses.
func selectSyncTarget(responses []latestDecisionResponse, required int) (latestDecisionResponse, bool) {
	candidates := make(map[string]*syncCandidate)
	for _, response := range responses {
		if !response.HaveTip {
			continue
		}
		key := fmt.Sprintf("%d:%d:%s", response.View, response.Sequence, response.Decision.Proposal.Digest())
		candidate, exists := candidates[key]
		if !exists {
			candidates[key] = &syncCandidate{response: response, votes: 1}
			continue
		}
		candidate.votes++
	}

	var best latestDecisionResponse
	found := false
	for _, candidate := range candidates {
		if candidate.votes < required {
			continue
		}
		if !found || candidate.response.Sequence > best.Sequence {
			best = candidate.response
			found = true
		}
	}
	return best, found
}

// fetchLatestDecisions queries every peer in parallel. Peers that fail to answer
// are simply absent from the result.
func (t *reportTransport) fetchLatestDecisions() []latestDecisionResponse {
	var (
		lock      sync.Mutex
		wg        sync.WaitGroup
		responses []latestDecisionResponse
	)

	request, err := fabrictransport.Marshal(&latestDecisionRequest{})
	if err != nil {
		fmt.Printf("%s sync: encoding tip request failed: %v\n", logTag("transport"), err)
		return nil
	}
	for id, client := range t.clients {
		wg.Add(1)
		go func(id uint64, client *fabrictransport.Client) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), reportSendTimeout)
			defer cancel()
			raw, err := client.Call(ctx, operationReportStateTransfer, request)
			if err != nil {
				fmt.Printf("%s sync: fetching tip from sharing node %d failed: %v\n",
					logTag("transport"), id, err)
				return
			}
			response := &latestDecisionResponse{}
			if err := fabrictransport.Unmarshal(raw, response); err != nil {
				fmt.Printf("%s sync: decoding tip from sharing node %d failed: %v\n",
					logTag("transport"), id, err)
				return
			}
			lock.Lock()
			responses = append(responses, *response)
			lock.Unlock()
		}(id, client)
	}
	wg.Wait()
	return responses
}
