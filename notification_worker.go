package main

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qnotify"
	"github.com/rqure/qlib/pkg/qdata/qstore/qnats"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type NotificationWorker interface {
	qapp.Worker
	OnBeforeStoreConnected()
	OnStoreConnected(context.Context)
	OnStoreDisconnected()
}

type notificationWorker struct {
	store            qdata.Store
	natsCore         qnats.Core
	isStoreConnected bool
	modeManager      ModeManager
	notifManager     NotificationManager
	handle           qapp.Handle
}

func NewNotificationWorker(store qdata.Store, natsCore qnats.Core, modeManager ModeManager, notifManager NotificationManager) NotificationWorker {
	return &notificationWorker{
		store:        store,
		natsCore:     natsCore,
		modeManager:  modeManager,
		notifManager: notifManager,
	}
}

func (me *notificationWorker) Init(ctx context.Context) {
	me.handle = qapp.GetHandle(ctx)
}

func (me *notificationWorker) Deinit(context.Context) {}
func (me *notificationWorker) DoWork(context.Context) {
	me.notifManager.ClearExpired()
}

func (me *notificationWorker) OnBeforeStoreConnected() {
	if me.modeManager.HasModes(ModeWrite) {
		me.natsCore.QueueSubscribe(qnats.NewKeyGenerator().GetNotificationRegistrationSubject(), me.handleNotificationRequest)
	}
}

func (me *notificationWorker) OnStoreConnected(context.Context) {
	me.isStoreConnected = true
}

func (me *notificationWorker) OnStoreDisconnected() {
	me.isStoreConnected = false
}

func (me *notificationWorker) handleNotificationRequest(msg *nats.Msg) {
	var apiMsg qprotobufs.ApiMessage
	if err := proto.Unmarshal(msg.Data, &apiMsg); err != nil {
		qlog.Error("Could not unmarshal message: %v", err)
		return
	}

	if apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeRegisterNotificationRequest{}) {
		me.handleRegisterNotification(msg, &apiMsg)
	} else if apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeUnregisterNotificationRequest{}) {
		me.handleUnregisterNotification(msg, &apiMsg)
	}
}

func (me *notificationWorker) handleRegisterNotification(msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeRegisterNotificationRequest)
	rsp := new(qprotobufs.ApiRuntimeRegisterNotificationResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		me.sendResponse(msg, rsp)
		return
	}

	if !me.isStoreConnected {
		qlog.Error("Could not handle request %v. Database is not connected.", req)
		me.sendResponse(msg, rsp)
		return
	}

	for _, cfg := range req.Requests {
		cfg := qnotify.FromConfigPb(cfg)
		me.notifManager.Register(cfg)
		rsp.Tokens = append(rsp.Tokens, cfg.GetToken())
	}

	me.sendResponse(msg, rsp)
}

func (me *notificationWorker) handleUnregisterNotification(msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeUnregisterNotificationRequest)
	rsp := new(qprotobufs.ApiRuntimeUnregisterNotificationResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		me.sendResponse(msg, rsp)
		return
	}

	if !me.isStoreConnected {
		qlog.Error("Could not handle request %v. Database is not connected.", req)
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

	if err := apiMsg.Payload.MarshalFrom(response); err != nil {
		qlog.Error("Could not marshal response: %v", err)
		return
	}

	data, err := proto.Marshal(apiMsg)
	if err != nil {
		qlog.Error("Could not marshal message: %v", err)
		return
	}

	if err := msg.Respond(data); err != nil {
		qlog.Error("Could not send response: %v", err)
	}
}
