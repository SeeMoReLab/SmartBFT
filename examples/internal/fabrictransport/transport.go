// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

// Package fabrictransport provides the stream lifecycle and buffering model
// used by Fabric's orderer networking in a form shared by the examples.
package fabrictransport

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

const (
	serviceName = "smartbft.examples.FabricTransport"
	stepMethod  = "/" + serviceName + "/Step"
)

var (
	// ErrOverflow is returned after a droppable operation fills its stream
	// queue. As in Fabric, the full stream is canceled and a later send creates
	// a fresh stream instead of allowing the caller to block the state machine.
	ErrOverflow = errors.New("fabric transport stream queue overflow")
	errClosed   = errors.New("fabric transport is closed")
	wireCodec   = gobCodec{}
)

func init() {
	encoding.RegisterCodec(wireCodec)
}

type gobCodec struct{}

func (gobCodec) Marshal(value any) ([]byte, error) {
	return Marshal(value)
}

func (gobCodec) Unmarshal(raw []byte, value any) error {
	return Unmarshal(raw, value)
}

func (gobCodec) Name() string {
	return "smartbft-fabric-gob"
}

// Marshal encodes an application payload carried inside an Envelope.
func Marshal(value any) ([]byte, error) {
	var out bytes.Buffer
	if err := gob.NewEncoder(&out).Encode(value); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// Unmarshal decodes an application payload carried inside an Envelope.
func Unmarshal(raw []byte, value any) error {
	return gob.NewDecoder(bytes.NewReader(raw)).Decode(value)
}

// Envelope is the common message transported by every operation stream.
// RequestID is zero for one-way traffic and non-zero for a request expecting a
// response on the same bidirectional stream.
type Envelope struct {
	From      uint64
	Operation string
	RequestID uint64
	Payload   []byte
	Error     string
}

// Handler dispatches one incoming transport envelope. Returning a payload or
// error only has a wire-visible effect for calls with a non-zero RequestID.
type Handler interface {
	Handle(context.Context, uint64, string, []byte) ([]byte, error)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, uint64, string, []byte) ([]byte, error)

func (f HandlerFunc) Handle(ctx context.Context, from uint64, operation string, payload []byte) ([]byte, error) {
	return f(ctx, from, operation, payload)
}

// ServerConfig controls response writes made by a registered Step service.
type ServerConfig struct {
	SendTimeout time.Duration
	OnError     func(operation string, err error)
}

type transportServiceServer interface {
	Step(transportStepServer) error
}

type registeredServer struct {
	handler Handler
	config  ServerConfig
}

type transportStepServer interface {
	Send(*Envelope) error
	Recv() (*Envelope, error)
	grpc.ServerStream
}

type stepServer struct {
	grpc.ServerStream
}

func (s *stepServer) Send(response *Envelope) error {
	return s.ServerStream.SendMsg(response)
}

func (s *stepServer) Recv() (*Envelope, error) {
	request := new(Envelope)
	if err := s.ServerStream.RecvMsg(request); err != nil {
		return nil, err
	}
	return request, nil
}

var stepStreamDesc = grpc.StreamDesc{
	StreamName:    "Step",
	Handler:       stepStreamHandler,
	ServerStreams: true,
	ClientStreams: true,
}

var transportServiceDesc = grpc.ServiceDesc{
	ServiceName: serviceName,
	HandlerType: (*transportServiceServer)(nil),
	Streams:     []grpc.StreamDesc{stepStreamDesc},
}

func stepStreamHandler(server any, stream grpc.ServerStream) error {
	return server.(transportServiceServer).Step(&stepServer{ServerStream: stream})
}

// RegisterServer installs the shared bidirectional Step service on a gRPC
// server. The gRPC server must not force a different codec, because the same
// server may also expose protobuf services such as the learning-agent API.
func RegisterServer(registrar grpc.ServiceRegistrar, handler Handler, config ServerConfig) error {
	if registrar == nil {
		return errors.New("fabric transport server registrar is nil")
	}
	if handler == nil {
		return errors.New("fabric transport handler is nil")
	}
	if config.SendTimeout <= 0 {
		return fmt.Errorf("fabric transport server send timeout must be positive: %s", config.SendTimeout)
	}
	registrar.RegisterService(&transportServiceDesc, &registeredServer{handler: handler, config: config})
	return nil
}

type receivedEnvelope struct {
	envelope *Envelope
	err      error
}

func (s *registeredServer) Step(stream transportStepServer) error {
	ctx := stream.Context()
	received := make(chan receivedEnvelope, 1)
	responses := make(chan *Envelope, 16)

	go func() {
		for {
			envelope, err := stream.Recv()
			select {
			case received <- receivedEnvelope{envelope: envelope, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case item := <-received:
			if item.err == io.EOF {
				return nil
			}
			if item.err != nil {
				return item.err
			}
			if item.envelope == nil || item.envelope.Operation == "" {
				return errors.New("received fabric transport envelope without an operation")
			}
			if item.envelope.RequestID == 0 {
				if _, err := s.handler.Handle(ctx, item.envelope.From, item.envelope.Operation, item.envelope.Payload); err != nil {
					s.report(item.envelope.Operation, err)
				}
				continue
			}

			envelope := item.envelope
			go func() {
				payload, err := s.handler.Handle(ctx, envelope.From, envelope.Operation, envelope.Payload)
				response := &Envelope{Operation: envelope.Operation, RequestID: envelope.RequestID, Payload: payload}
				if err != nil {
					response.Error = err.Error()
				}
				select {
				case responses <- response:
				case <-ctx.Done():
				}
			}()

		case response := <-responses:
			if err := sendServerResponse(stream, response, s.config.SendTimeout); err != nil {
				s.report(response.Operation, err)
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func sendServerResponse(stream transportStepServer, response *Envelope, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() {
		done <- stream.Send(response)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return fmt.Errorf("sending %s response timed out after %s", response.Operation, timeout)
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

func (s *registeredServer) report(operation string, err error) {
	if s.config.OnError != nil && err != nil {
		s.config.OnError(operation, err)
	}
}

// ClientConfig configures all operation streams to one remote endpoint.
type ClientConfig struct {
	SelfID            uint64
	Address           string
	QueueSize         int
	SendTimeout       time.Duration
	MaxReceiveMsgSize int
	OnError           func(operation string, err error)
	Dialer            func(context.Context, string) (net.Conn, error)
}

// Client owns one lazily-created stream per operation to one destination.
type Client struct {
	selfID      uint64
	queueSize   int
	sendTimeout time.Duration
	onError     func(operation string, err error)
	conn        *grpc.ClientConn
	lock        sync.Mutex
	streams     map[string]*operationStream
	closed      bool
	nextID      atomic.Uint64
}

// NewClient creates an insecure gRPC client. Connections and operation streams
// connect lazily, so a temporarily unavailable endpoint does not prevent a node
// from starting.
func NewClient(config ClientConfig) (*Client, error) {
	if config.Address == "" {
		return nil, errors.New("fabric transport client address is empty")
	}
	if config.QueueSize <= 0 {
		return nil, fmt.Errorf("fabric transport queue size must be positive: %d", config.QueueSize)
	}
	if config.SendTimeout <= 0 {
		return nil, fmt.Errorf("fabric transport send timeout must be positive: %s", config.SendTimeout)
	}

	callOptions := []grpc.CallOption{
		grpc.ForceCodec(wireCodec),
	}
	if config.MaxReceiveMsgSize > 0 {
		callOptions = append(callOptions, grpc.MaxCallRecvMsgSize(config.MaxReceiveMsgSize))
	}
	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(callOptions...),
	}
	if config.Dialer != nil {
		dialOptions = append(dialOptions, grpc.WithContextDialer(config.Dialer))
	}
	conn, err := grpc.NewClient(config.Address, dialOptions...)
	if err != nil {
		return nil, err
	}
	return &Client{
		selfID:      config.SelfID,
		queueSize:   config.QueueSize,
		sendTimeout: config.SendTimeout,
		onError:     config.OnError,
		conn:        conn,
		streams:     make(map[string]*operationStream),
	}, nil
}

// Send enqueues a one-way message. If allowDrop is true, a full queue cancels
// the stream and returns ErrOverflow. If it is false, Send waits for queue space
// or transport shutdown, matching Fabric's submit-stream behavior.
func (c *Client) Send(operation string, payload []byte, allowDrop bool, report func(error)) error {
	if operation == "" {
		return errors.New("fabric transport send operation is empty")
	}
	stream, err := c.getOrCreateStream(operation)
	if err != nil {
		return err
	}
	envelope := &Envelope{
		From:      c.selfID,
		Operation: operation,
		Payload:   append([]byte(nil), payload...),
	}
	if err := stream.send(outbound{envelope: envelope, report: report}, allowDrop, nil); err != nil {
		c.removeStream(operation, stream)
		return err
	}
	return nil
}

// Call sends a request and waits for its correlated response on the same
// operation stream. A caller deadline stops waiting without canceling unrelated
// calls sharing the stream.
func (c *Client) Call(ctx context.Context, operation string, payload []byte) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("fabric transport call context is nil")
	}
	if operation == "" {
		return nil, errors.New("fabric transport call operation is empty")
	}
	stream, err := c.getOrCreateStream(operation)
	if err != nil {
		return nil, err
	}
	requestID := c.nextID.Add(1)
	response := make(chan callResult, 1)
	stream.addPending(requestID, response)
	envelope := &Envelope{
		From:      c.selfID,
		Operation: operation,
		RequestID: requestID,
		Payload:   append([]byte(nil), payload...),
	}
	if err := stream.send(outbound{envelope: envelope}, false, ctx); err != nil {
		stream.removePending(requestID)
		return nil, err
	}

	select {
	case result := <-response:
		return result.payload, result.err
	case <-ctx.Done():
		stream.removePending(requestID)
		return nil, ctx.Err()
	}
}

func (c *Client) getOrCreateStream(operation string) (*operationStream, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.closed {
		return nil, errClosed
	}
	if stream := c.streams[operation]; stream != nil && !stream.isCanceled() {
		return stream, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	clientStream, err := c.conn.NewStream(ctx, &stepStreamDesc, stepMethod)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open %s stream: %w", operation, err)
	}
	stream := &operationStream{
		client:      c,
		operation:   operation,
		stream:      clientStream,
		cancel:      cancel,
		sendTimeout: c.sendTimeout,
		sendBuff:    make(chan outbound, c.queueSize),
		abort:       make(chan struct{}),
		pending:     make(map[uint64]chan callResult),
	}
	c.streams[operation] = stream
	stream.start()
	return stream, nil
}

func (c *Client) removeStream(operation string, expected *operationStream) {
	c.lock.Lock()
	if c.streams[operation] == expected {
		delete(c.streams, operation)
	}
	c.lock.Unlock()
}

// Close cancels every operation stream and closes the underlying connection.
func (c *Client) Close() error {
	c.lock.Lock()
	if c.closed {
		c.lock.Unlock()
		return nil
	}
	c.closed = true
	streams := make([]*operationStream, 0, len(c.streams))
	for _, stream := range c.streams {
		streams = append(streams, stream)
	}
	c.streams = make(map[string]*operationStream)
	c.lock.Unlock()

	for _, stream := range streams {
		stream.cancelWith(errClosed)
	}
	for _, stream := range streams {
		stream.wait()
	}
	return c.conn.Close()
}

func (c *Client) report(operation string, err error) {
	if c.onError != nil && err != nil {
		c.onError(operation, err)
	}
}

type outbound struct {
	envelope *Envelope
	report   func(error)
}

type callResult struct {
	payload []byte
	err     error
}

type operationStream struct {
	client      *Client
	operation   string
	stream      grpc.ClientStream
	cancel      context.CancelFunc
	sendTimeout time.Duration
	sendBuff    chan outbound
	sendLock    sync.Mutex
	abort       chan struct{}
	cancelOnce  sync.Once
	wg          sync.WaitGroup
	pendingLock sync.Mutex
	pending     map[uint64]chan callResult
	errLock     sync.Mutex
	abortErr    error
	canceled    atomic.Bool
}

func (s *operationStream) start() {
	s.wg.Add(2)
	go s.serviceSends()
	go s.serviceResponses()
}

func (s *operationStream) send(message outbound, allowDrop bool, caller context.Context) error {
	s.sendLock.Lock()
	defer s.sendLock.Unlock()
	if s.isCanceled() {
		return s.reason()
	}
	if allowDrop && len(s.sendBuff) == cap(s.sendBuff) {
		s.cancelWith(ErrOverflow)
		return ErrOverflow
	}
	var callerDone <-chan struct{}
	if caller != nil {
		callerDone = caller.Done()
	}
	select {
	case s.sendBuff <- message:
		return nil
	case <-s.abort:
		return s.reason()
	case <-callerDone:
		return caller.Err()
	}
}

func (s *operationStream) serviceSends() {
	defer s.wg.Done()
	defer s.failQueued()
	for {
		select {
		case message := <-s.sendBuff:
			err := s.sendMessage(message.envelope)
			if message.report != nil {
				message.report(err)
			}
			if err != nil {
				if s.cancelWith(err) {
					s.client.report(s.operation, err)
				}
				return
			}
		case <-s.abort:
			return
		}
	}
}

func (s *operationStream) sendMessage(envelope *Envelope) error {
	done := make(chan error, 1)
	go func() {
		done <- s.stream.SendMsg(envelope)
	}()
	timer := time.NewTimer(s.sendTimeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return fmt.Errorf("sending %s message timed out after %s", s.operation, s.sendTimeout)
	case <-s.abort:
		return s.reason()
	}
}

func (s *operationStream) serviceResponses() {
	defer s.wg.Done()
	for {
		response := new(Envelope)
		if err := s.stream.RecvMsg(response); err != nil {
			if s.cancelWith(err) {
				s.client.report(s.operation, err)
			}
			return
		}
		if response.RequestID == 0 {
			continue
		}
		result := callResult{payload: response.Payload}
		if response.Error != "" {
			result.err = errors.New(response.Error)
		}
		s.completePending(response.RequestID, result)
	}
}

func (s *operationStream) addPending(id uint64, response chan callResult) {
	s.pendingLock.Lock()
	s.pending[id] = response
	s.pendingLock.Unlock()
}

func (s *operationStream) removePending(id uint64) {
	s.pendingLock.Lock()
	delete(s.pending, id)
	s.pendingLock.Unlock()
}

func (s *operationStream) completePending(id uint64, result callResult) {
	s.pendingLock.Lock()
	response := s.pending[id]
	delete(s.pending, id)
	s.pendingLock.Unlock()
	if response != nil {
		response <- result
	}
}

func (s *operationStream) failPending(err error) {
	s.pendingLock.Lock()
	pending := s.pending
	s.pending = make(map[uint64]chan callResult)
	s.pendingLock.Unlock()
	for _, response := range pending {
		response <- callResult{err: err}
	}
}

func (s *operationStream) failQueued() {
	err := s.reason()
	for {
		select {
		case message := <-s.sendBuff:
			if message.report != nil {
				message.report(err)
			}
		default:
			return
		}
	}
}

func (s *operationStream) cancelWith(err error) bool {
	if err == nil {
		err = errors.New("fabric transport stream canceled")
	}
	canceled := false
	s.cancelOnce.Do(func() {
		canceled = true
		s.errLock.Lock()
		s.abortErr = err
		s.errLock.Unlock()
		s.canceled.Store(true)
		s.cancel()
		close(s.abort)
		s.failPending(err)
		s.client.removeStream(s.operation, s)
	})
	return canceled
}

func (s *operationStream) isCanceled() bool {
	return s.canceled.Load()
}

func (s *operationStream) reason() error {
	s.errLock.Lock()
	defer s.errLock.Unlock()
	if s.abortErr != nil {
		return s.abortErr
	}
	return errors.New("fabric transport stream canceled")
}

func (s *operationStream) wait() {
	s.wg.Wait()
}
