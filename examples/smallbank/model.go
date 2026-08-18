// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

type txType string

const (
	txDepositChecking txType = "DEPOSIT_CHECKING"
	txTransactSavings txType = "TRANSACT_SAVINGS"
	txWriteCheck      txType = "WRITE_CHECK"
	txSendPayment     txType = "SEND_PAYMENT"
	txAmalgamate      txType = "AMALGAMATE"
	txBalance         txType = "BALANCE"
	txCreateAccount   txType = "CREATE_ACCOUNT"
)

var weightedTxTypes = [6]txType{
	txDepositChecking,
	txTransactSavings,
	txWriteCheck,
	txSendPayment,
	txAmalgamate,
	txBalance,
}

type request struct {
	ClientID             string `json:"client_id"`
	ID                   string `json:"id"`
	Type                 txType `json:"type"`
	CustomerID           uint64 `json:"customer_id,omitempty"`
	CustomerName         string `json:"customer_name,omitempty"`
	DestCustomerID       uint64 `json:"dest_customer_id,omitempty"`
	AmountCents          int64  `json:"amount_cents,omitempty"`
	SavingsBalanceCents  int64  `json:"savings_balance_cents,omitempty"`
	CheckingBalanceCents int64  `json:"checking_balance_cents,omitempty"`
}

type responseStatus string

const (
	statusSuccess           responseStatus = "SUCCESS"
	statusInsufficientFunds responseStatus = "INSUFFICIENT_FUNDS"
	statusBusinessError     responseStatus = "BUSINESS_ERROR"
	statusSystemError       responseStatus = "SYSTEM_ERROR"
	// statusDuplicate is returned for a request whose client sequence number was
	// already executed. The original outcome is replayed when it is still
	// cached; otherwise only the status is returned.
	statusDuplicate responseStatus = "DUPLICATE"
)

type response struct {
	ClientID             string         `json:"client_id"`
	ID                   string         `json:"id"`
	Status               responseStatus `json:"status"`
	Error                string         `json:"error,omitempty"`
	SavingsBalanceCents  int64          `json:"savings_balance_cents,omitempty"`
	CheckingBalanceCents int64          `json:"checking_balance_cents,omitempty"`
}

func (r response) benchmarkSuccess() bool {
	return r.Status == statusSuccess || r.Status == statusInsufficientFunds || r.Status == statusDuplicate
}

func encodeRequest(req request) ([]byte, error) {
	return json.Marshal(req)
}

func decodeRequest(raw []byte) (request, error) {
	var req request
	if err := json.Unmarshal(raw, &req); err != nil {
		return request{}, err
	}
	if req.ClientID == "" || req.ID == "" {
		return request{}, fmt.Errorf("request missing client_id or id")
	}
	return req, nil
}

type blockData struct {
	Requests [][]byte `json:"requests"`
}

func encodeBlockData(data blockData) []byte {
	raw, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	return raw
}

func decodeBlockData(raw []byte) (*blockData, error) {
	var data blockData
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	return &data, nil
}

type blockHeader struct {
	Sequence int64  `json:"sequence"`
	PrevHash string `json:"prev_hash"`
	DataHash string `json:"data_hash"`
}

func encodeBlockHeader(header blockHeader) []byte {
	raw, err := json.Marshal(header)
	if err != nil {
		panic(err)
	}
	return raw
}

func decodeBlockHeader(raw []byte) (*blockHeader, error) {
	var header blockHeader
	if err := json.Unmarshal(raw, &header); err != nil {
		return nil, err
	}
	return &header, nil
}

func hashBytes(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

type smallBankState struct {
	accounts map[uint64]string
	checking map[uint64]int64
	savings  map[uint64]int64

	// lastExecuted holds, per client, the highest client sequence number whose
	// request has been executed, and lastResponse the outcome of that request.
	// Consensus can deliver the same request more than once - a copy that
	// arrives after the original was committed can be re-admitted to a request
	// pool once the pool's short-lived processed-request cache has evicted it,
	// and requests stranded in a pool by a state-transfer sync are forwarded
	// and proposed again. Executing such a replay would corrupt balances
	// identically on every replica, so it would not show up as a state
	// mismatch. These two maps make execution at-most-once per client sequence
	// number, and are part of the replicated state: they are covered by the
	// state checksum and travel in the sync snapshot, so every replica reaches
	// the same decision for the same request.
	lastExecuted map[string]uint64
	lastResponse map[string]response

	// duplicatesSkipped counts replays that were prevented from executing. It
	// is a diagnostic only: replicas legitimately disagree on it, so it is kept
	// out of the snapshot and the state checksum.
	duplicatesSkipped uint64
}

type accountSnapshot struct {
	CustomerID           uint64 `json:"customer_id"`
	CustomerName         string `json:"customer_name"`
	CheckingBalanceCents int64  `json:"checking_balance_cents"`
	SavingsBalanceCents  int64  `json:"savings_balance_cents"`
}

// clientSnapshot carries one client's execution watermark and last outcome.
type clientSnapshot struct {
	ClientID string   `json:"client_id"`
	LastID   uint64   `json:"last_id"`
	Response response `json:"response"`
}

// stateSnapshot is the deterministic, checksummed image of the whole
// application state.
type stateSnapshot struct {
	Accounts []accountSnapshot `json:"accounts"`
	Clients  []clientSnapshot  `json:"clients"`
}

func newSmallBankState() *smallBankState {
	return &smallBankState{
		accounts:     make(map[uint64]string),
		checking:     make(map[uint64]int64),
		savings:      make(map[uint64]int64),
		lastExecuted: make(map[string]uint64),
		lastResponse: make(map[string]response),
	}
}

// parseClientSeq extracts the client sequence number from a request ID. Request
// IDs must be canonical unsigned decimal counters that increase per client,
// which is what makes them usable as execution watermarks.
func parseClientSeq(id string) (uint64, error) {
	seq, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid request sequence %q: %w", id, err)
	}
	if strconv.FormatUint(seq, 10) != id {
		return 0, fmt.Errorf("request sequence %q is not canonical", id)
	}
	return seq, nil
}

// isExecuted reports whether the client sequence number has already been
// executed. Request validation rejects invalid sequence numbers before they
// reach this check.
func (s *smallBankState) isExecuted(clientID, requestID string) bool {
	seq, err := parseClientSeq(requestID)
	if err != nil {
		return false
	}
	last, seen := s.lastExecuted[clientID]
	return seen && seq <= last
}

// apply executes a request at most once per client sequence number. A request
// whose sequence number was already executed does not touch the state; its
// recorded outcome is replayed instead.
func (s *smallBankState) apply(req request) response {
	seq, err := parseClientSeq(req.ID)
	if err != nil {
		return response{
			ClientID: req.ClientID,
			ID:       req.ID,
			Status:   statusSystemError,
			Error:    err.Error(),
		}
	}

	if s.isExecuted(req.ClientID, req.ID) {
		s.duplicatesSkipped++
		if cached, hit := s.lastResponse[req.ClientID]; hit && cached.ID == req.ID {
			return cached
		}
		// Only the most recent outcome per client is retained, so an older
		// replay can no longer be answered with its original result.
		return response{
			ClientID: req.ClientID,
			ID:       req.ID,
			Status:   statusDuplicate,
		}
	}

	resp := s.execute(req)
	s.lastExecuted[req.ClientID] = seq
	s.lastResponse[req.ClientID] = resp
	return resp
}

func (s *smallBankState) execute(req request) response {
	resp := response{
		ClientID: req.ClientID,
		ID:       req.ID,
		Status:   statusSuccess,
	}

	switch req.Type {
	case txCreateAccount:
		if _, exists := s.accounts[req.CustomerID]; exists {
			return resp.withError(statusBusinessError, "account already exists")
		}
		s.accounts[req.CustomerID] = req.CustomerName
		s.checking[req.CustomerID] = req.CheckingBalanceCents
		s.savings[req.CustomerID] = req.SavingsBalanceCents
	case txDepositChecking:
		if !s.accountExists(req.CustomerID) {
			return resp.withError(statusBusinessError, "account not found")
		}
		s.checking[req.CustomerID] += req.AmountCents
	case txTransactSavings:
		if !s.accountExists(req.CustomerID) {
			return resp.withError(statusBusinessError, "account not found")
		}
		next := s.savings[req.CustomerID] + req.AmountCents
		if next < 0 {
			return resp.withError(statusInsufficientFunds, "insufficient funds")
		}
		s.savings[req.CustomerID] = next
	case txWriteCheck:
		if !s.accountExists(req.CustomerID) {
			return resp.withError(statusBusinessError, "account not found")
		}
		next := s.checking[req.CustomerID] - req.AmountCents
		if next < 0 {
			return resp.withError(statusInsufficientFunds, "insufficient funds")
		}
		s.checking[req.CustomerID] = next
	case txSendPayment:
		if !s.accountExists(req.CustomerID) {
			return resp.withError(statusBusinessError, "source account not found")
		}
		if !s.accountExists(req.DestCustomerID) {
			return resp.withError(statusBusinessError, "destination account not found")
		}
		next := s.checking[req.CustomerID] - req.AmountCents
		if next < 0 {
			return resp.withError(statusInsufficientFunds, "insufficient funds")
		}
		s.checking[req.CustomerID] = next
		s.checking[req.DestCustomerID] += req.AmountCents
	case txAmalgamate:
		if !s.accountExists(req.CustomerID) {
			return resp.withError(statusBusinessError, "account 1 not found")
		}
		if !s.accountExists(req.DestCustomerID) {
			return resp.withError(statusBusinessError, "account 2 not found")
		}
		amount := s.checking[req.DestCustomerID]
		s.checking[req.DestCustomerID] = 0
		s.savings[req.CustomerID] += amount
	case txBalance:
		if !s.accountExists(req.CustomerID) {
			return resp.withError(statusBusinessError, "account not found")
		}
		resp.CheckingBalanceCents = s.checking[req.CustomerID]
		resp.SavingsBalanceCents = s.savings[req.CustomerID]
	default:
		return resp.withError(statusSystemError, "unknown transaction type")
	}

	return resp
}

func (s *smallBankState) accountExists(id uint64) bool {
	_, exists := s.accounts[id]
	return exists
}

func (s *smallBankState) deterministicSnapshot() []accountSnapshot {
	ids := make([]uint64, 0, len(s.accounts))
	for id := range s.accounts {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	snapshot := make([]accountSnapshot, 0, len(ids))
	for _, id := range ids {
		snapshot = append(snapshot, accountSnapshot{
			CustomerID:           id,
			CustomerName:         s.accounts[id],
			CheckingBalanceCents: s.checking[id],
			SavingsBalanceCents:  s.savings[id],
		})
	}
	return snapshot
}

func (s *smallBankState) deterministicClientSnapshot() []clientSnapshot {
	clients := make([]string, 0, len(s.lastExecuted))
	for clientID := range s.lastExecuted {
		clients = append(clients, clientID)
	}
	sort.Strings(clients)

	snapshot := make([]clientSnapshot, 0, len(clients))
	for _, clientID := range clients {
		snapshot = append(snapshot, clientSnapshot{
			ClientID: clientID,
			LastID:   s.lastExecuted[clientID],
			Response: s.lastResponse[clientID],
		})
	}
	return snapshot
}

// deterministicStateSnapshot is the image the state checksum is computed over.
// It must cover every field that execution depends on, including the client
// watermarks: a replica that restored only balances would re-execute a replay
// that the others skip, and the states would then genuinely diverge.
func (s *smallBankState) deterministicStateSnapshot() stateSnapshot {
	return stateSnapshot{
		Accounts: s.deterministicSnapshot(),
		Clients:  s.deterministicClientSnapshot(),
	}
}

func (r response) withError(status responseStatus, message string) response {
	r.Status = status
	r.Error = message
	return r
}

func requestKey(clientID, requestID string) string {
	return clientID + ":" + requestID
}
