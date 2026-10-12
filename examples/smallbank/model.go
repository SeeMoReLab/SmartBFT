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
	"strings"
)

// requestSizeBytes is the exact length of every encoded request. The padding
// field absorbs the difference, so the payload size does not depend on the
// digit counts of the client and request IDs.
const requestSizeBytes = 512

// request carries no operation: execution only advances the client's
// watermark, so the workload exercises consensus with a fixed payload size.
type request struct {
	ClientID string `json:"client_id"`
	ID       string `json:"id"`
	Padding  string `json:"padding"`
}

type responseStatus string

const (
	statusSuccess     responseStatus = "SUCCESS"
	statusSystemError responseStatus = "SYSTEM_ERROR"
	// statusDuplicate is returned for a request whose client sequence number was
	// already executed. The original outcome is replayed when it is still
	// cached; otherwise only the status is returned.
	statusDuplicate responseStatus = "DUPLICATE"
)

type response struct {
	ClientID string         `json:"client_id"`
	ID       string         `json:"id"`
	Status   responseStatus `json:"status"`
	Error    string         `json:"error,omitempty"`
}

func (r response) benchmarkSuccess() bool {
	return r.Status == statusSuccess || r.Status == statusDuplicate
}

// encodeRequest pads req so that its encoding is exactly requestSizeBytes. The
// padding is sized on the unpadded encoding of each request, since the IDs
// change length as the counters grow.
func encodeRequest(req request) ([]byte, error) {
	req.Padding = ""
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	if len(raw) > requestSizeBytes {
		return nil, fmt.Errorf("unpadded request is %d bytes, exceeds the %d-byte request size", len(raw), requestSizeBytes)
	}
	req.Padding = strings.Repeat("x", requestSizeBytes-len(raw))
	raw, err = json.Marshal(req)
	if err != nil {
		return nil, err
	}
	if len(raw) != requestSizeBytes {
		return nil, fmt.Errorf("padded request is %d bytes, want %d", len(raw), requestSizeBytes)
	}
	return raw, nil
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

func hashBytes(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

type smallBankState struct {
	// lastExecuted holds, per client, the highest client sequence number whose
	// request has been executed, and lastResponse the outcome of that request.
	// Consensus can deliver the same request more than once - a copy that
	// arrives after the original was committed can be re-admitted to a request
	// pool once the pool's short-lived processed-request cache has evicted it,
	// and requests stranded in a pool by a state-transfer sync are forwarded
	// and proposed again. These two maps make execution at-most-once per client sequence
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

// clientSnapshot carries one client's execution watermark and last outcome.
type clientSnapshot struct {
	ClientID string   `json:"client_id"`
	LastID   uint64   `json:"last_id"`
	Response response `json:"response"`
}

// stateSnapshot is the deterministic, checksummed image of the whole
// application state.
type stateSnapshot struct {
	Clients []clientSnapshot `json:"clients"`
}

func newSmallBankState() *smallBankState {
	return &smallBankState{
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

	resp := response{
		ClientID: req.ClientID,
		ID:       req.ID,
		Status:   statusSuccess,
	}
	s.lastExecuted[req.ClientID] = seq
	s.lastResponse[req.ClientID] = resp
	return resp
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
// It must cover every field that execution depends on, which is the client
// watermarks: a replica without them would re-execute a replay that the others
// skip, and the states would then genuinely diverge.
func (s *smallBankState) deterministicStateSnapshot() stateSnapshot {
	return stateSnapshot{
		Clients: s.deterministicClientSnapshot(),
	}
}

func requestKey(clientID, requestID string) string {
	return clientID + ":" + requestID
}
