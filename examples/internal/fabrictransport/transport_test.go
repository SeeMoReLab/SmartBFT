// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package fabrictransport

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestCallReusesOperationStreamAndRecreatesCanceledStream(t *testing.T) {
	listener := newPipeListener()
	server := grpc.NewServer()
	err := RegisterServer(server, HandlerFunc(func(_ context.Context, from uint64, operation string, payload []byte) ([]byte, error) {
		if from != 7 {
			return nil, errors.New("unexpected sender")
		}
		if operation == "fail" {
			return nil, errors.New("handler failed")
		}
		return append([]byte(operation+":"), payload...), nil
	}), ServerConfig{SendTimeout: time.Second})
	if err != nil {
		t.Fatalf("register server: %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-serveDone
	})

	client, err := NewClient(ClientConfig{
		SelfID:      7,
		Address:     "passthrough:///fabrictransport-test",
		QueueSize:   4,
		SendTimeout: time.Second,
		Dialer: func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		},
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	call := func(operation, payload string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		response, err := client.Call(ctx, operation, []byte(payload))
		if err != nil {
			t.Fatalf("call %s: %v", operation, err)
		}
		return string(response)
	}

	if got := call("consensus", "one"); got != "consensus:one" {
		t.Fatalf("first response = %q", got)
	}
	first := client.streams["consensus"]
	if got := call("consensus", "two"); got != "consensus:two" {
		t.Fatalf("second response = %q", got)
	}
	if client.streams["consensus"] != first {
		t.Fatal("same operation did not reuse its stream")
	}
	if got := call("state-transfer", "tip"); got != "state-transfer:tip" {
		t.Fatalf("state-transfer response = %q", got)
	}
	if client.streams["state-transfer"] == first {
		t.Fatal("different operations unexpectedly shared a stream")
	}

	first.cancelWith(errors.New("test cancellation"))
	if got := call("consensus", "three"); got != "consensus:three" {
		t.Fatalf("response after recreation = %q", got)
	}
	if client.streams["consensus"] == first {
		t.Fatal("canceled stream was not recreated")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := client.Call(ctx, "fail", nil); err == nil || err.Error() != "handler failed" {
		t.Fatalf("handler error = %v", err)
	}
}

func TestDroppableOverflowCancelsWithoutBlocking(t *testing.T) {
	stream, entered := newBlockedOperationStream(1)
	if err := stream.send(outbound{envelope: &Envelope{Operation: "consensus"}}, true, nil); err != nil {
		t.Fatalf("first send: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("stream worker did not start its send")
	}
	if err := stream.send(outbound{envelope: &Envelope{Operation: "consensus"}}, true, nil); err != nil {
		t.Fatalf("second send: %v", err)
	}

	started := time.Now()
	err := stream.send(outbound{envelope: &Envelope{Operation: "consensus"}}, true, nil)
	if !errors.Is(err, ErrOverflow) {
		t.Fatalf("overflow error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("overflow blocked for %s", elapsed)
	}
	if !stream.isCanceled() {
		t.Fatal("overflow did not cancel the stream")
	}
	stream.wait()
}

func TestNonDroppableSendWaitsUntilStreamCancellation(t *testing.T) {
	stream, entered := newBlockedOperationStream(1)
	if err := stream.send(outbound{envelope: &Envelope{Operation: "transaction"}}, false, nil); err != nil {
		t.Fatalf("first send: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("stream worker did not start its send")
	}
	if err := stream.send(outbound{envelope: &Envelope{Operation: "transaction"}}, false, nil); err != nil {
		t.Fatalf("second send: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- stream.send(outbound{envelope: &Envelope{Operation: "transaction"}}, false, nil)
	}()
	select {
	case err := <-done:
		t.Fatalf("non-droppable send returned before queue space or cancellation: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	stream.cancelWith(errors.New("network failed"))
	select {
	case err := <-done:
		if err == nil || err.Error() != "network failed" {
			t.Fatalf("blocked send error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked send did not stop after stream cancellation")
	}
	stream.wait()
}

func newBlockedOperationStream(queueSize int) (*operationStream, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{}, 1)
	client := &Client{streams: make(map[string]*operationStream)}
	stream := &operationStream{
		client:      client,
		operation:   "test",
		stream:      &blockedClientStream{ctx: ctx, entered: entered},
		cancel:      cancel,
		sendTimeout: time.Hour,
		sendBuff:    make(chan outbound, queueSize),
		abort:       make(chan struct{}),
		pending:     make(map[uint64]chan callResult),
	}
	client.streams[stream.operation] = stream
	stream.start()
	return stream, entered
}

type blockedClientStream struct {
	ctx     context.Context
	entered chan<- struct{}
}

func (s *blockedClientStream) Header() (metadata.MD, error) { return nil, nil }
func (s *blockedClientStream) Trailer() metadata.MD         { return nil }
func (s *blockedClientStream) CloseSend() error             { return nil }
func (s *blockedClientStream) Context() context.Context     { return s.ctx }
func (s *blockedClientStream) SendMsg(any) error {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.ctx.Done()
	return s.ctx.Err()
}
func (s *blockedClientStream) RecvMsg(any) error {
	<-s.ctx.Done()
	return s.ctx.Err()
}

type pipeListener struct {
	connections chan net.Conn
	closed      chan struct{}
	closeOnce   sync.Once
}

func newPipeListener() *pipeListener {
	return &pipeListener{connections: make(chan net.Conn), closed: make(chan struct{})}
}

func (l *pipeListener) Dial() (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case l.connections <- server:
		return client, nil
	case <-l.closed:
		_ = client.Close()
		_ = server.Close()
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case connection := <-l.connections:
		return connection, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (l *pipeListener) Addr() net.Addr {
	return pipeAddress("fabrictransport-test")
}

type pipeAddress string

func (a pipeAddress) Network() string { return "pipe" }
func (a pipeAddress) String() string  { return string(a) }
