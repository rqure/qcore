package main

import (
	"context"
	"log"
	"net/http"

	"github.com/coder/websocket"
	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"github.com/rqure/qlib/pkg/qss"
	"google.golang.org/protobuf/proto"
)

const DefaultAddr = ":7860"

type ClientConnectedArgs struct {
	Ctx  context.Context
	Conn *websocket.Conn
}

type ClientDisconnectedArgs struct {
	Ctx  context.Context
	Conn *websocket.Conn
}

type MessageReceivedArgs struct {
	Ctx  context.Context
	Conn *websocket.Conn
	Msg  *qprotobufs.ApiMessage
}

type ConnectionWorker interface {
	qapp.Worker

	ClientConnected() qss.Signal[ClientConnectedArgs]
	ClientDisconnected() qss.Signal[ClientDisconnectedArgs]
	MessageReceived() qss.Signal[MessageReceivedArgs]
}

type connectionWorker struct {
	server *http.Server
	done   chan struct{}
	handle qcontext.Handle

	clientConnected    qss.Signal[ClientConnectedArgs]
	clientDisconnected qss.Signal[ClientDisconnectedArgs]
	messageReceived    qss.Signal[MessageReceivedArgs]
}

func NewConnectionWorker() ConnectionWorker {
	return &connectionWorker{
		done:               make(chan struct{}),
		clientConnected:    qss.New[ClientConnectedArgs](),
		clientDisconnected: qss.New[ClientDisconnectedArgs](),
		messageReceived:    qss.New[MessageReceivedArgs](),
	}
}

func (me *connectionWorker) ClientConnected() qss.Signal[ClientConnectedArgs] {
	return me.clientConnected
}

func (me *connectionWorker) ClientDisconnected() qss.Signal[ClientDisconnectedArgs] {
	return me.clientDisconnected
}

func (me *connectionWorker) MessageReceived() qss.Signal[MessageReceivedArgs] {
	return me.messageReceived
}

func (me *connectionWorker) Init(ctx context.Context) {
	me.handle = qcontext.GetHandle(ctx)

	// Create a new HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/", me.handleWebSocket)

	// Initialize the HTTP server
	me.server = &http.Server{
		Addr:    DefaultAddr, // You can change the port as needed
		Handler: mux,
	}

	// Start the server in a separate goroutine
	go func() {
		qlog.Info("Starting WebSocket server on %s", DefaultAddr)
		if err := me.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			qlog.Panic("WebSocket server error: %v", err)
		}
	}()
}

func (me *connectionWorker) Deinit(ctx context.Context) {
	if me.server != nil {
		log.Println("Shutting down WebSocket server")
		if err := me.server.Shutdown(ctx); err != nil {
			qlog.Error("WebSocket server shutdown error: %v", err)
		}
	}

	close(me.done)
}

func (me *connectionWorker) DoWork(ctx context.Context) {
}

// handleWebSocket handles WebSocket connections
func (me *connectionWorker) handleWebSocket(rw http.ResponseWriter, r *http.Request) {
	r = r.WithContext(me.handle.Ctx())

	conn, err := websocket.Accept(rw, r, nil)
	if err != nil {
		qlog.Warn("Failed to upgrade connection: %v", err)
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	qlog.Info("Client connected: %s", r.RemoteAddr)
	me.clientConnected.Emit(ClientConnectedArgs{Ctx: r.Context(), Conn: conn})

	// Simple echo server for demonstration
	for {
		messageType, p, err := conn.Read(r.Context())
		if err != nil {
			qlog.Warn("Error reading message: %v", err)
			break
		}

		if messageType != websocket.MessageBinary {
			qlog.Warn("Unexpected message type: %d", messageType)
			continue
		}

		msg := &qprotobufs.ApiMessage{}
		if err := proto.Unmarshal(p, msg); err != nil {
			qlog.Warn("Failed to unmarshal message: %v", err)
			continue
		}

		if msg.Header == nil {
			qlog.Warn("Message header is nil")
			continue
		}

		if msg.Payload == nil {
			qlog.Warn("Message payload is nil")
			continue
		}

		qlog.Trace("Received message: %v", p)
		me.messageReceived.Emit(MessageReceivedArgs{Ctx: r.Context(), Conn: conn, Msg: msg})
	}

	qlog.Info("Client disconnected: %s", r.RemoteAddr)
	me.clientDisconnected.Emit(ClientDisconnectedArgs{Ctx: r.Context(), Conn: conn})
}
