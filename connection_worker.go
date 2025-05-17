package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	"github.com/coder/websocket"
	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qauthentication"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"github.com/rqure/qlib/pkg/qss"
	"google.golang.org/protobuf/proto"
)

const DefaultAddr = ":7860"

// AuthRequest represents credentials sent in authentication requests
type AuthRequest struct {
	Username     string `json:"username"`
	Password     string `json:"password"`
	RefreshToken string `json:"refreshToken,omitempty"`
}

// AuthResponse represents the response to an authentication request
type AuthResponse struct {
	Success      bool   `json:"success"`
	Username     string `json:"username,omitempty"`
	AccessToken  string `json:"accessToken,omitempty"`
	RefreshToken string `json:"refreshToken,omitempty"`
	Message      string `json:"message,omitempty"`
}

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

	OnStoreInteractorConnected(qdata.ConnectedArgs)
	OnStoreInteractorDisconnected(qdata.DisconnectedArgs)
}

type connectionWorker struct {
	server *http.Server
	done   chan struct{}
	handle qcontext.Handle

	clientConnected    qss.Signal[ClientConnectedArgs]
	clientDisconnected qss.Signal[ClientDisconnectedArgs]
	messageReceived    qss.Signal[MessageReceivedArgs]

	isStoreConnected bool

	rwMu *sync.RWMutex
}

func NewConnectionWorker() ConnectionWorker {
	return &connectionWorker{
		done:               make(chan struct{}),
		clientConnected:    qss.New[ClientConnectedArgs](),
		clientDisconnected: qss.New[ClientDisconnectedArgs](),
		messageReceived:    qss.New[MessageReceivedArgs](),
		isStoreConnected:   false,
		rwMu:               &sync.RWMutex{},
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

func (me *connectionWorker) OnStoreInteractorConnected(args qdata.ConnectedArgs) {
	me.rwMu.Lock()
	defer me.rwMu.Unlock()
	me.isStoreConnected = true
}

func (me *connectionWorker) OnStoreInteractorDisconnected(args qdata.DisconnectedArgs) {
	me.rwMu.Lock()
	defer me.rwMu.Unlock()
	me.isStoreConnected = false
}

func (me *connectionWorker) IsStoreConnected() bool {
	me.rwMu.RLock()
	defer me.rwMu.RUnlock()
	return me.isStoreConnected
}

func (me *connectionWorker) Init(ctx context.Context) {
	me.handle = qcontext.GetHandle(ctx)

	// Create a new HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/core/", me.handleWebSocket)

	// Register the auth handler
	mux.HandleFunc("/core/auth/", me.handleAuth)

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
		qlog.Info("Shutting down WebSocket server")

		if err := me.server.Shutdown(ctx); err != nil {
			qlog.Warn("WebSocket server shutdown error: %v", err)
		} else {
			qlog.Info("WebSocket server shut down successfully")
		}
	}

	close(me.done)
}

func (me *connectionWorker) DoWork(ctx context.Context) {
}

// handleWebSocket handles WebSocket connections
func (me *connectionWorker) handleWebSocket(rw http.ResponseWriter, r *http.Request) {
	if !me.IsStoreConnected() {
		qlog.Warn("WebSocket server is not ready")
		http.Error(rw, "Server not ready", http.StatusServiceUnavailable)
		return
	}

	r = r.WithContext(me.handle.Ctx())

	// Configure WebSocket options to allow any origin
	wsOptions := &websocket.AcceptOptions{
		// Allow all origins during development
		InsecureSkipVerify: true,
	}

	conn, err := websocket.Accept(rw, r, wsOptions)
	if err != nil {
		qlog.Warn("Failed to upgrade connection: %v", err)
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	qlog.Info("Client connected: %s", r.RemoteAddr)
	me.clientConnected.Emit(ClientConnectedArgs{Ctx: r.Context(), Conn: conn})

	// Simple echo server for demonstration
	for me.IsStoreConnected() {
		messageType, p, err := conn.Read(r.Context())

		if err != nil {
			if strings.Contains(err.Error(), "EOF") {
				break
			}

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

// handleAuth handles authentication requests
func (me *connectionWorker) handleAuth(rw http.ResponseWriter, r *http.Request) {
	// Set CORS headers to allow cross-origin requests
	origin := r.Header.Get("Origin")
	if origin == "" {
		// If no origin is set, allow all origins during development
		origin = "*"
	}

	// Set comprehensive CORS headers
	rw.Header().Set("Access-Control-Allow-Origin", origin)
	rw.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	rw.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With")
	rw.Header().Set("Access-Control-Allow-Credentials", "true")
	rw.Header().Set("Access-Control-Max-Age", "86400") // 24 hours cache for preflight

	// Handle preflight OPTIONS requests
	if r.Method == http.MethodOptions {
		rw.WriteHeader(http.StatusOK)
		return
	}

	// Only allow POST method for authentication
	if r.Method != http.MethodPost {
		http.Error(rw, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Check if the store is connected before proceeding
	if !me.IsStoreConnected() {
		qlog.Warn("Auth server is not ready")
		http.Error(rw, "Server not ready", http.StatusServiceUnavailable)
		return
	}

	// Parse the authentication request
	var authReq AuthRequest
	err := json.NewDecoder(r.Body).Decode(&authReq)
	if err != nil {
		qlog.Warn("Failed to parse auth request: %v", err)
		http.Error(rw, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Check for a refresh token
	if authReq.RefreshToken != "" {
		rspCh := make(chan AuthResponse, 1)
		me.handle.DoInMainThread(func(ctx context.Context) {
			clientProvider := qcontext.GetClientProvider[qauthentication.Client](ctx)
			client := clientProvider.Client(ctx)
			if client == nil {
				qlog.Warn("Client not found")
				rspCh <- AuthResponse{
					Success: false,
					Message: "Client not found",
				}
				return
			}
			session := client.RefreshTokenToSession(ctx, authReq.RefreshToken)
			if session == nil || !session.CheckIsValid(ctx) {
				qlog.Warn("Invalid session")
				rspCh <- AuthResponse{
					Success: false,
					Message: "Invalid session",
				}
				return
			}

			rspCh <- AuthResponse{
				Success:      true,
				Username:     authReq.Username,
				AccessToken:  session.AccessToken(),
				RefreshToken: session.RefreshToken(),
				Message:      "Authentication successful",
			}
		})
		rsp := <-rspCh
		if !rsp.Success {
			sendAuthResponse(rw, rsp, http.StatusUnauthorized)
			return
		}

		qlog.Info("User '%s' authenticated with refresh token", authReq.Username)
		sendAuthResponse(rw, rsp, http.StatusOK)
		return
	}

	// Validate required fields
	if authReq.Username == "" || authReq.Password == "" {
		sendAuthResponse(rw, AuthResponse{
			Success: false,
			Message: "Username and password are required",
		}, http.StatusBadRequest)
		return
	}

	// Use the existing context with the request context
	rspCh := make(chan AuthResponse, 1)
	me.handle.DoInMainThread(func(ctx context.Context) {
		clientProvider := qcontext.GetClientProvider[qauthentication.Client](ctx)
		client := clientProvider.Client(ctx)
		if client == nil {
			qlog.Warn("Client not found")
			rspCh <- AuthResponse{
				Success: false,
				Message: "Client not found",
			}
			return
		}
		session := client.CreateUserSession(ctx, authReq.Username, authReq.Password)
		if session == nil {
			qlog.Warn("Failed to create user session")
			rspCh <- AuthResponse{
				Success: false,
				Message: "Failed to create user session",
			}
			return
		}
		if !session.CheckIsValid(ctx) {
			qlog.Warn("Invalid session")
			rspCh <- AuthResponse{
				Success: false,
				Message: "Invalid session, supplied credentials are invalid or user does not exist",
			}
			return
		}

		rspCh <- AuthResponse{
			Success:      true,
			Username:     authReq.Username,
			AccessToken:  session.AccessToken(),
			RefreshToken: session.RefreshToken(),
			Message:      "Authentication successful",
		}
	})

	rsp := <-rspCh
	if !rsp.Success {
		sendAuthResponse(rw, rsp, http.StatusUnauthorized)
		return
	}

	qlog.Info("User authenticated: %s", authReq.Username)
	sendAuthResponse(rw, rsp, http.StatusOK)
}

// sendAuthResponse sends a JSON response with appropriate headers and status code
func sendAuthResponse(rw http.ResponseWriter, response AuthResponse, statusCode int) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(statusCode)

	// Encode and send the response
	if err := json.NewEncoder(rw).Encode(response); err != nil {
		qlog.Warn("Failed to encode auth response: %v", err)
	}
}
