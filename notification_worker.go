package main

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qnotify"
	"github.com/rqure/qlib/pkg/qdata/qstore/qnats"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type NotificationWorker interface {
	qapp.Worker
	OnBeforeStoreConnected(qdata.ConnectedArgs)
}

type notificationWorker struct {
	store        *qdata.Store
	natsCore     qnats.NatsCore
	modeManager  ModeManager
	notifManager NotificationManager
	handle       qcontext.Handle
}

func NewNotificationWorker(store *qdata.Store, natsCore qnats.NatsCore, modeManager ModeManager, notifManager NotificationManager) NotificationWorker {
	return &notificationWorker{
		store:        store,
		natsCore:     natsCore,
		modeManager:  modeManager,
		notifManager: notifManager,
	}
}

func (me *notificationWorker) Init(ctx context.Context) {
	me.handle = qcontext.GetHandle(ctx)
}

func (me *notificationWorker) Deinit(context.Context) {}
func (me *notificationWorker) DoWork(context.Context) {
	me.notifManager.ClearExpired()
}

func (me *notificationWorker) OnBeforeStoreConnected(args qdata.ConnectedArgs) {
	if me.modeManager.HasModes(ModeWrite) {
		me.natsCore.QueueSubscribe(
			qnats.NewKeyGenerator().GetNotificationRegistrationSubject(),
			qcontext.GetAppName(args.Ctx),
			me.handleNotificationRequest)
	}
}

func (me *notificationWorker) handleNotificationRequest(msg *nats.Msg) {
	var apiMsg qprotobufs.ApiMessage
	if err := proto.Unmarshal(msg.Data, &apiMsg); err != nil {
		qlog.Warn("Could not unmarshal message: %v", err)
		return
	}

	me.handle.DoInMainThread(func(context.Context) {
		if apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeRegisterNotificationRequest{}) {
			me.handleRegisterNotification(msg, &apiMsg)
		} else if apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeUnregisterNotificationRequest{}) {
			me.handleUnregisterNotification(msg, &apiMsg)
		}
	})
}

func (me *notificationWorker) handleRegisterNotification(msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeRegisterNotificationRequest)
	rsp := new(qprotobufs.ApiRuntimeRegisterNotificationResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeRegisterNotificationResponse_FAILURE
		me.sendResponse(msg, rsp)
		return
	}

	for _, cfg := range req.Requests {
		cfg := qnotify.FromConfigPb(cfg)
		me.notifManager.Register(cfg)
		rsp.Tokens = append(rsp.Tokens, cfg.GetToken())
	}

	rsp.Status = qprotobufs.ApiRuntimeRegisterNotificationResponse_SUCCESS
	me.sendResponse(msg, rsp)
}

func (me *notificationWorker) handleUnregisterNotification(msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeUnregisterNotificationRequest)
	rsp := new(qprotobufs.ApiRuntimeUnregisterNotificationResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Warn("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiRuntimeUnregisterNotificationResponse_FAILURE
		me.sendResponse(msg, rsp)
		return
	}

	for _, token := range req.Tokens {
		me.notifManager.Unregister(qnotify.FromToken(token))
	}

	rsp.Status = qprotobufs.ApiRuntimeUnregisterNotificationResponse_SUCCESS
	me.sendResponse(msg, rsp)
}

func (me *notificationWorker) sendResponse(msg *nats.Msg, response proto.Message) {
	if msg.Reply == "" {
		return
	}

	apiMsg := &qprotobufs.ApiMessage{
		Header: &qprotobufs.ApiHeader{
			Timestamp: timestamppb.Now(),
		},
	}

	var err error
	apiMsg.Payload, err = anypb.New(response)
	if err != nil {
		qlog.Warn("Could not marshal response: %v", err)
		return
	}

	data, err := proto.Marshal(apiMsg)
	if err != nil {
		qlog.Warn("Could not marshal message: %v", err)
		return
	}

	if err := msg.Respond(data); err != nil {
		qlog.Warn("Could not send response: %v", err)
	}
}
