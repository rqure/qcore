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

func (w *notificationWorker) Init(ctx context.Context) {
	w.handle = qapp.GetHandle(ctx)
}

func (w *notificationWorker) Deinit(context.Context) {}
func (w *notificationWorker) DoWork(context.Context) {
	w.notifManager.ClearExpired()
}

func (w *notificationWorker) OnStoreConnected(ctx context.Context) {
	w.isStoreConnected = true

	if w.modeManager.HasModes(ModeWrite) {
		w.natsCore.QueueSubscribe(qnats.NewKeyGenerator().GetNotificationRegistrationSubject(), w.handleNotificationRequest)
	}
}
func (w *notificationWorker) OnStoreDisconnected() {
	w.isStoreConnected = false
}

func (w *notificationWorker) handleNotificationRequest(msg *nats.Msg) {
	w.handle.DoInMainThread(func(ctx context.Context) {
		var apiMsg qprotobufs.ApiMessage
		if err := proto.Unmarshal(msg.Data, &apiMsg); err != nil {
			qlog.Error("Could not unmarshal message: %v", err)
			return
		}

		if apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeRegisterNotificationRequest{}) {
			w.handleRegisterNotification(ctx, msg, &apiMsg)
		} else if apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeUnregisterNotificationRequest{}) {
			w.handleUnregisterNotification(ctx, msg, &apiMsg)
		}
	})
}

func (w *notificationWorker) handleRegisterNotification(_ context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeRegisterNotificationRequest)
	rsp := new(qprotobufs.ApiRuntimeRegisterNotificationResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isStoreConnected {
		qlog.Error("Could not handle request %v. Database is not connected.", req)
		w.sendResponse(msg, rsp)
		return
	}

	for _, cfg := range req.Requests {
		cfg := qnotify.FromConfigPb(cfg)
		w.notifManager.Register(cfg)
		rsp.Tokens = append(rsp.Tokens, cfg.GetToken())
	}

	w.sendResponse(msg, rsp)
}

func (w *notificationWorker) handleUnregisterNotification(_ context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeUnregisterNotificationRequest)
	rsp := new(qprotobufs.ApiRuntimeUnregisterNotificationResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isStoreConnected {
		qlog.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiRuntimeUnregisterNotificationResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	for _, token := range req.Tokens {
		w.notifManager.Unregister(qnotify.FromToken(token))
	}

	rsp.Status = qprotobufs.ApiRuntimeUnregisterNotificationResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *notificationWorker) sendResponse(msg *nats.Msg, response proto.Message) {
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
