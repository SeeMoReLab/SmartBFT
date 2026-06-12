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
	return r.Status == statusSuccess || r.Status == statusInsufficientFunds
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

func hashBytes(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

type smallBankState struct {
	accounts map[uint64]string
	checking map[uint64]int64
	savings  map[uint64]int64
}

func newSmallBankState() *smallBankState {
	return &smallBankState{
		accounts: make(map[uint64]string),
		checking: make(map[uint64]int64),
		savings:  make(map[uint64]int64),
	}
}

func (s *smallBankState) apply(req request) response {
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

func (r response) withError(status responseStatus, message string) response {
	r.Status = status
	r.Error = message
	return r
}

func requestKey(clientID, requestID string) string {
	return clientID + ":" + requestID
}
