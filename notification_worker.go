package main

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/notification"
	qnats "github.com/rqure/qlib/pkg/data/store/nats"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type NotificationWorker interface {
	app.Worker
	OnStoreConnected(context.Context)
	OnStoreDisconnected()
}

type notificationWorker struct {
	store            data.Store
	natsCore         qnats.Core
	isStoreConnected bool
	modeManager      ModeManager
	notifManager     NotificationManager
	handle           app.Handle
}

func NewNotificationWorker(store data.Store, natsCore qnats.Core, modeManager ModeManager, notifManager NotificationManager) NotificationWorker {
	return &notificationWorker{
		store:        store,
		natsCore:     natsCore,
		modeManager:  modeManager,
		notifManager: notifManager,
	}
}

func (w *notificationWorker) Init(ctx context.Context, handle app.Handle) {
	w.handle = handle

	if !w.modeManager.HasModes(ModeWrite) {
		return
	}

	w.natsCore.QueueSubscribe(qnats.NewKeyGenerator().GetNotificationSubject(), w.handleNotificationRequest)
}

func (w *notificationWorker) Deinit(context.Context) {}
func (w *notificationWorker) DoWork(context.Context) {
	w.notifManager.ClearExpired()
}

func (w *notificationWorker) OnStoreConnected(ctx context.Context) {
	w.isStoreConnected = true
}
func (w *notificationWorker) OnStoreDisconnected() {
	w.isStoreConnected = false
}

func (w *notificationWorker) handleNotificationRequest(msg *nats.Msg) {
	w.handle.DoInMainThread(func(ctx context.Context) {
		var apiMsg protobufs.ApiMessage
		if err := proto.Unmarshal(msg.Data, &apiMsg); err != nil {
			log.Error("Could not unmarshal message: %v", err)
			return
		}

		if apiMsg.Payload.MessageIs(&protobufs.ApiRuntimeRegisterNotificationRequest{}) {
			w.handleRegisterNotification(ctx, msg, &apiMsg)
		} else if apiMsg.Payload.MessageIs(&protobufs.ApiRuntimeUnregisterNotificationRequest{}) {
			w.handleUnregisterNotification(ctx, msg, &apiMsg)
		}
	})
}

func (w *notificationWorker) handleRegisterNotification(ctx context.Context, msg *nats.Msg, apiMsg *protobufs.ApiMessage) {
	req := new(protobufs.ApiRuntimeRegisterNotificationRequest)
	rsp := new(protobufs.ApiRuntimeRegisterNotificationResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		w.sendResponse(msg, rsp)
		return
	}

	for _, cfg := range req.Requests {
		cfg := notification.FromConfigPb(cfg)
		w.notifManager.Register(cfg)
		rsp.Tokens = append(rsp.Tokens, cfg.GetToken())
	}

	w.sendResponse(msg, rsp)
}

func (w *notificationWorker) handleUnregisterNotification(ctx context.Context, msg *nats.Msg, apiMsg *protobufs.ApiMessage) {
	req := new(protobufs.ApiRuntimeUnregisterNotificationRequest)
	rsp := new(protobufs.ApiRuntimeUnregisterNotificationResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = protobufs.ApiRuntimeUnregisterNotificationResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	for _, token := range req.Tokens {
		w.notifManager.Unregister(notification.FromToken(token))
	}

	rsp.Status = protobufs.ApiRuntimeUnregisterNotificationResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *notificationWorker) sendResponse(msg *nats.Msg, response proto.Message) {
	if msg.Reply == "" {
		return
	}

	apiMsg := &protobufs.ApiMessage{
		Header: &protobufs.ApiHeader{
			AuthenticationStatus: protobufs.ApiHeader_AUTHENTICATED,
			Timestamp:            timestamppb.Now(),
		},
	}

	if err := apiMsg.Payload.MarshalFrom(response); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	data, err := proto.Marshal(apiMsg)
	if err != nil {
		log.Error("Could not marshal message: %v", err)
		return
	}

	if err := msg.Respond(data); err != nil {
		log.Error("Could not send response: %v", err)
	}
}
