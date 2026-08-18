// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"errors"
	"testing"

	bft "github.com/hyperledger-labs/SmartBFT/pkg/types"
)

func seedAccounts(t *testing.T, state *smallBankState) {
	t.Helper()
	for _, id := range []uint64{1, 2} {
		req := request{
			ClientID:             "create-0",
			ID:                   itoa(id),
			Type:                 txCreateAccount,
			CustomerID:           id,
			CustomerName:         "customer",
			CheckingBalanceCents: 1_000_000,
			SavingsBalanceCents:  1_000_000,
		}
		if resp := state.apply(req); resp.Status != statusSuccess {
			t.Fatalf("seeding account %d failed: %+v", id, resp)
		}
	}
}

func itoa(v uint64) string {
	digits := ""
	if v == 0 {
		return "0"
	}
	for v > 0 {
		digits = string(rune('0'+v%10)) + digits
		v /= 10
	}
	return digits
}

func payment(clientID, id string) request {
	return request{
		ClientID:       clientID,
		ID:             id,
		Type:           txSendPayment,
		CustomerID:     1,
		DestCustomerID: 2,
		AmountCents:    500,
	}
}

func TestApplyExecutesEachClientSequenceOnce(t *testing.T) {
	state := newSmallBankState()
	seedAccounts(t, state)

	first := state.apply(payment("terminal-1", "10"))
	if first.Status != statusSuccess {
		t.Fatalf("expected the first execution to succeed, got %+v", first)
	}
	checking, savings := state.checking[1], state.checking[2]

	replay := state.apply(payment("terminal-1", "10"))
	if state.checking[1] != checking || state.checking[2] != savings {
		t.Fatalf("replay moved funds: checking %d -> %d, dest %d -> %d",
			checking, state.checking[1], savings, state.checking[2])
	}
	if replay != first {
		t.Fatalf("expected the recorded outcome to be replayed, got %+v want %+v", replay, first)
	}

	// A newer request advances the watermark; the older replay can no longer be
	// answered from the cache but must still not execute.
	if resp := state.apply(payment("terminal-1", "11")); resp.Status != statusSuccess {
		t.Fatalf("expected the next sequence number to execute, got %+v", resp)
	}
	checking, savings = state.checking[1], state.checking[2]

	stale := state.apply(payment("terminal-1", "10"))
	if stale.Status != statusDuplicate {
		t.Fatalf("expected a stale replay to report a duplicate, got %+v", stale)
	}
	if state.checking[1] != checking || state.checking[2] != savings {
		t.Fatalf("stale replay moved funds: checking %d -> %d, dest %d -> %d",
			checking, state.checking[1], savings, state.checking[2])
	}
}

func TestClientsAreIndependent(t *testing.T) {
	state := newSmallBankState()
	seedAccounts(t, state)

	if resp := state.apply(payment("terminal-1", "10")); resp.Status != statusSuccess {
		t.Fatalf("terminal-1 should execute, got %+v", resp)
	}
	// Request IDs come from a counter shared by all terminals, so a lower
	// sequence number from a different client is not a duplicate.
	if resp := state.apply(payment("terminal-2", "4")); resp.Status != statusSuccess {
		t.Fatalf("terminal-2 should execute, got %+v", resp)
	}
}

func TestWatermarksSurviveStateTransfer(t *testing.T) {
	state := newSmallBankState()
	seedAccounts(t, state)
	if resp := state.apply(payment("terminal-1", "10")); resp.Status != statusSuccess {
		t.Fatalf("expected execution, got %+v", resp)
	}

	image := state.deterministicStateSnapshot()
	restored, err := stateFromSnapshots(image.Accounts, image.Clients)
	if err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}

	checking, dest := restored.checking[1], restored.checking[2]
	if resp := restored.apply(payment("terminal-1", "10")); resp.Status != statusSuccess {
		t.Fatalf("expected the cached outcome after restore, got %+v", resp)
	}
	if restored.checking[1] != checking || restored.checking[2] != dest {
		t.Fatal("replay executed on a state-transferred replica")
	}
	if hashBytes(mustJSON(restored.deterministicStateSnapshot())) != hashBytes(mustJSON(image)) {
		t.Fatal("restored state does not reproduce the source checksum")
	}
}

func TestChecksumCoversWatermarks(t *testing.T) {
	state := newSmallBankState()
	seedAccounts(t, state)
	if resp := state.apply(payment("terminal-1", "10")); resp.Status != statusSuccess {
		t.Fatalf("expected execution, got %+v", resp)
	}

	withWatermarks := hashBytes(mustJSON(state.deterministicStateSnapshot()))

	// Same balances, no execution history: a replica in this state would
	// re-execute a replay that the others skip, so the checksums must differ.
	stripped, err := stateFromSnapshots(state.deterministicSnapshot(), nil)
	if err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	if hashBytes(mustJSON(stripped.deterministicStateSnapshot())) == withWatermarks {
		t.Fatal("state checksum ignores client watermarks")
	}
}

func TestSnapshotIsDeterministic(t *testing.T) {
	state := newSmallBankState()
	seedAccounts(t, state)
	for _, client := range []string{"terminal-9", "terminal-3", "terminal-11", "terminal-1"} {
		if resp := state.apply(payment(client, "10")); resp.Status != statusSuccess {
			t.Fatalf("%s should execute, got %+v", client, resp)
		}
	}

	want := hashBytes(mustJSON(state.deterministicStateSnapshot()))
	for i := 0; i < 20; i++ {
		if got := hashBytes(mustJSON(state.deterministicStateSnapshot())); got != want {
			t.Fatalf("snapshot is not stable across map iterations: %s != %s", got, want)
		}
	}

	clients := state.deterministicClientSnapshot()
	for i := 1; i < len(clients); i++ {
		if clients[i-1].ClientID >= clients[i].ClientID {
			t.Fatalf("client snapshot is not sorted: %s before %s", clients[i-1].ClientID, clients[i].ClientID)
		}
	}
}

func TestInvalidRequestIDsDoNotExecute(t *testing.T) {
	state := newSmallBankState()
	seedAccounts(t, state)

	checking, dest := state.checking[1], state.checking[2]
	for _, id := range []string{"not-a-number", "01", "+1", "18446744073709551616"} {
		if resp := state.apply(payment("terminal-1", id)); resp.Status != statusSystemError {
			t.Fatalf("invalid request ID %q must fail, got %+v", id, resp)
		}
		if state.checking[1] != checking || state.checking[2] != dest {
			t.Fatalf("invalid request ID %q moved funds", id)
		}
		if _, tracked := state.lastExecuted["terminal-1"]; tracked {
			t.Fatalf("invalid request ID %q set a watermark", id)
		}
	}
}

// TestBatchReplayIsSkipped exercises the loop Deliver runs over a decoded
// batch: the same batch delivered twice must move funds once.
func TestBatchReplayIsSkipped(t *testing.T) {
	state := newSmallBankState()
	seedAccounts(t, state)

	batch := []request{
		payment("terminal-1", "10"),
		payment("terminal-2", "11"),
		payment("terminal-3", "12"),
	}
	for _, req := range batch {
		if resp := state.apply(req); resp.Status != statusSuccess {
			t.Fatalf("first delivery failed for %s: %+v", req.ClientID, resp)
		}
	}
	checking, dest := state.checking[1], state.checking[2]

	for _, req := range batch {
		state.apply(req)
	}
	if state.checking[1] != checking || state.checking[2] != dest {
		t.Fatalf("batch replay moved funds: %d -> %d, %d -> %d",
			checking, state.checking[1], dest, state.checking[2])
	}
	if state.duplicatesSkipped != uint64(len(batch)) {
		t.Fatalf("expected %d skipped duplicates, got %d", len(batch), state.duplicatesSkipped)
	}
}

// TestIsExecutedFiltersPoolAdmission covers the filter the consensus layer uses
// to keep executed requests out of the request pool, on both the client submit
// and the peer forwarding path.
func TestIsExecutedFiltersPoolAdmission(t *testing.T) {
	state := newSmallBankState()
	seedAccounts(t, state)

	if resp := state.apply(payment("terminal-1", "10")); resp.Status != statusSuccess {
		t.Fatalf("expected execution, got %+v", resp)
	}

	if !state.isExecuted("terminal-1", "10") {
		t.Fatal("the executed sequence number must be filtered")
	}
	if !state.isExecuted("terminal-1", "9") {
		t.Fatal("a sequence number below the watermark must be filtered")
	}
	if state.isExecuted("terminal-1", "11") {
		t.Fatal("a sequence number above the watermark must be admitted")
	}
	if state.isExecuted("terminal-2", "10") {
		t.Fatal("another client's sequence number must be admitted")
	}
	if state.isExecuted("terminal-1", "not-a-number") {
		t.Fatal("an invalid request sequence must not be reported as executed")
	}
}

// testNode builds a node with just enough wiring to exercise the admission
// filter, which only needs the application state.
func testNode(t *testing.T) *node {
	t.Helper()
	n := &node{id: 1, state: newSmallBankState()}
	seedAccounts(t, n.state)
	return n
}

func TestVerifyRequestRejectsExecutedRequest(t *testing.T) {
	n := testNode(t)

	raw, err := encodeRequest(payment("terminal-1", "10"))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Before execution the request is admitted.
	if _, err := n.VerifyRequest(raw); err != nil {
		t.Fatalf("expected a fresh request to be admitted, got %v", err)
	}

	if resp := n.state.apply(payment("terminal-1", "10")); resp.Status != statusSuccess {
		t.Fatalf("expected execution, got %+v", resp)
	}

	// Afterwards the forwarding path must refuse it.
	if _, err := n.VerifyRequest(raw); !errors.Is(err, errRequestAlreadyExecuted) {
		t.Fatalf("expected the executed request to be rejected, got %v", err)
	}

	// The client submission path resolves the retry from replicated state
	// without admitting or executing it again.
	if err := n.submitRequest("terminal-1", "10", raw); err != nil {
		t.Fatalf("expected the executed client retry to be resolved, got %v", err)
	}
}

func TestVerifyProposalAcceptsExecutedRequest(t *testing.T) {
	n := testNode(t)

	raw, err := encodeRequest(payment("terminal-1", "10"))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if resp := n.state.apply(payment("terminal-1", "10")); resp.Status != statusSuccess {
		t.Fatalf("expected execution, got %+v", resp)
	}

	// A duplicate inside a proposal must not invalidate the proposal: doing so
	// would depose a leader for including a harmless replay. It is skipped at
	// execution instead.
	proposal := bft.Proposal{Payload: encodeBlockData(blockData{Requests: [][]byte{raw}})}
	infos, err := n.VerifyProposal(proposal)
	if err != nil {
		t.Fatalf("proposal containing an executed request must verify, got %v", err)
	}
	if len(infos) != 1 || infos[0].ClientID != "terminal-1" || infos[0].ID != "10" {
		t.Fatalf("unexpected request infos: %+v", infos)
	}
}

func TestVerifyRequestStillRejectsMalformed(t *testing.T) {
	n := testNode(t)

	if _, err := n.VerifyRequest([]byte("not json")); err == nil {
		t.Fatal("expected malformed payload to be rejected")
	}

	bad := payment("terminal-1", "10")
	bad.Type = "NOT_A_TX"
	raw, err := encodeRequest(bad)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := n.VerifyRequest(raw); err == nil {
		t.Fatal("expected an unknown transaction type to be rejected")
	}

	for _, id := range []string{"not-a-number", "01", "+1", "18446744073709551616"} {
		req := payment("terminal-1", id)
		raw, err := encodeRequest(req)
		if err != nil {
			t.Fatalf("encode request %q: %v", id, err)
		}
		if _, err := n.VerifyRequest(raw); err == nil {
			t.Fatalf("expected invalid request sequence %q to be rejected", id)
		}
		if err := n.submitRequest(req.ClientID, req.ID, raw); err == nil {
			t.Fatalf("expected direct submission with invalid request sequence %q to be rejected", id)
		}

		proposal := bft.Proposal{Payload: encodeBlockData(blockData{Requests: [][]byte{raw}})}
		if _, err := n.VerifyProposal(proposal); err == nil {
			t.Fatalf("expected proposal with invalid request sequence %q to be rejected", id)
		}
	}
}
