package main

import (
	"context"

	"github.com/coder/websocket"
	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qnotify"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type NotificationWorker interface {
	qapp.Worker
	OnMessageReceived(args MessageReceivedArgs)
	OnClientConnected(args ClientConnectedArgs)
	OnClientDisconnected(args ClientDisconnectedArgs)
}

type notificationWorker struct {
	store        *qdata.Store
	notifManager NotificationManager
	handle       qcontext.Handle
}

func NewNotificationWorker(store *qdata.Store, notifManager NotificationManager) NotificationWorker {
	return &notificationWorker{
		store:        store,
		notifManager: notifManager,
	}
}

func (me *notificationWorker) Init(ctx context.Context) {
	me.handle = qcontext.GetHandle(ctx)
}

func (me *notificationWorker) Deinit(context.Context) {}
func (me *notificationWorker) DoWork(context.Context) {}

func (me *notificationWorker) OnClientConnected(args ClientConnectedArgs) {
}

func (me *notificationWorker) OnClientDisconnected(args ClientDisconnectedArgs) {
}

func (me *notificationWorker) OnMessageReceived(args MessageReceivedArgs) {
	switch {
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeRegisterNotificationRequest{}):
		me.handleRegisterNotification(args)
	case args.Msg.Payload.MessageIs(&qprotobufs.ApiRuntimeUnregisterNotificationRequest{}):
		me.handleUnregisterNotification(args)
	default:
	}
}

// Refactored to use MessageReceivedArgs
func (me *notificationWorker) handleRegisterNotification(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeRegisterNotificationRequest)
	rsp := new(qprotobufs.ApiRuntimeRegisterNotificationResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeRegisterNotificationResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	authCtx, ok := verifyAuthentication(args.Ctx, args.Msg.Header.AccessToken, me.store)
	if !ok {
		qlog.Warn("Authentication failed")
		rsp.Status = qprotobufs.ApiRuntimeRegisterNotificationResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}
	authorizer, ok := qcontext.GetAuthorizer(authCtx)
	if !ok {
		qlog.Warn("Could not get authorizer from context")
		rsp.Status = qprotobufs.ApiRuntimeRegisterNotificationResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	for _, cfgPb := range req.Requests {
		cfg := qnotify.FromConfigPb(cfgPb)
		rsp.Tokens = append(rsp.Tokens, cfg.GetToken())
		me.notifManager.Register(args.Conn, authorizer, cfg)
	}

	rsp.Status = qprotobufs.ApiRuntimeRegisterNotificationResponse_SUCCESS
	me.sendResponse(args, rsp)
}

func (me *notificationWorker) handleUnregisterNotification(args MessageReceivedArgs) {
	req := new(qprotobufs.ApiRuntimeUnregisterNotificationRequest)
	rsp := new(qprotobufs.ApiRuntimeUnregisterNotificationResponse)

	if err := args.Msg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeUnregisterNotificationResponse_FAILURE
		me.sendResponse(args, rsp)
		return
	}

	_, ok := verifyAuthentication(args.Ctx, args.Msg.Header.AccessToken, me.store)
	if !ok {
		return
	}

	for _, token := range req.Tokens {
		me.notifManager.Unregister(args.Conn, qnotify.FromToken(token))
	}

	rsp.Status = qprotobufs.ApiRuntimeUnregisterNotificationResponse_SUCCESS
	me.sendResponse(args, rsp)
}

// Send response using websocket connection from MessageReceivedArgs
func (me *notificationWorker) sendResponse(args MessageReceivedArgs, response proto.Message) {
	args.Msg.Header.Timestamp = timestamppb.Now()

	var err error
	args.Msg.Payload, err = anypb.New(response)
	if err != nil {
		qlog.Warn("Could not marshal response: %v", err)
		return
	}

	data, err := proto.Marshal(args.Msg)
	if err != nil {
		qlog.Warn("Could not marshal message: %v", err)
		return
	}

	if err := args.Conn.Write(args.Ctx, websocket.MessageBinary, data); err != nil {
		qlog.Warn("Could not send response: %v", err)
	}
}
