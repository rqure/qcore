package main

import (
	"context"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/data/query"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	web "github.com/rqure/qlib/pkg/web/go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type RuntimeWorker struct {
	store            data.Store
	isStoreConnected bool

	modeManager ModeManager

	clientNotificationQueue  map[string]map[string][]data.Notification
	clientNotificationTokens map[string]map[string]data.NotificationToken
}

func NewRuntimeWorker(store data.Store, modeManager ModeManager) *RuntimeWorker {
	return &RuntimeWorker{
		store:                    store,
		isStoreConnected:         false,
		modeManager:              modeManager,
		clientNotificationQueue:  make(map[string]map[string][]data.Notification),
		clientNotificationTokens: make(map[string]map[string]data.NotificationToken),
	}
}

func (w *RuntimeWorker) Init(context.Context, app.Handle) {

}

func (w *RuntimeWorker) Deinit(context.Context) {

}

func (w *RuntimeWorker) DoWork(context.Context) {

}

func (w *RuntimeWorker) OnClientConnected(ctx context.Context, args ...interface{}) {
	client := args[0].(web.Client)
	w.clientNotificationQueue[client.Id()] = make(map[string][]data.Notification)
	w.clientNotificationTokens[client.Id()] = make(map[string]data.NotificationToken)
}

func (w *RuntimeWorker) OnClientDisconnected(ctx context.Context, args ...interface{}) {
	clientId := args[0].(string)

	for _, token := range w.clientNotificationTokens[clientId] {
		log.Info("Unbinding notification token: %v", token)
		token.Unbind(ctx)
	}

	delete(w.clientNotificationQueue, clientId)
	delete(w.clientNotificationTokens, clientId)
}

func (w *RuntimeWorker) OnNewClientMessage(ctx context.Context, args ...interface{}) {
	client := args[0].(web.Client)
	msg := args[1].(web.Message)

	if msg.Payload.MessageIs(&protobufs.ApiRuntimeDatabaseRequest{}) {
		w.onRuntimeDatabaseRequest(ctx, client, msg)
	} else if msg.Payload.MessageIs(&protobufs.ApiRuntimeRegisterNotificationRequest{}) {
		w.onRuntimeRegisterNotificationRequest(ctx, client, msg)
	} else if msg.Payload.MessageIs(&protobufs.ApiRuntimeUnregisterNotificationRequest{}) {
		w.onRuntimeUnregisterNotificationRequest(ctx, client, msg)
	} else if msg.Payload.MessageIs(&protobufs.ApiRuntimeGetNotificationsRequest{}) {
		w.onRuntimeGetNotificationsRequest(ctx, client, msg)
	} else if msg.Payload.MessageIs(&protobufs.ApiRuntimeGetDatabaseConnectionStatusRequest{}) {
		w.onRuntimeGetDatabaseConnectionStatusRequest(ctx, client, msg)
	} else if msg.Payload.MessageIs(&protobufs.ApiRuntimeGetEntitiesRequest{}) {
		w.onRuntimeGetEntitiesRequest(ctx, client, msg)
	} else if msg.Payload.MessageIs(&protobufs.ApiRuntimeFieldExistsRequest{}) {
		w.onRuntimeFieldExistsRequest(ctx, client, msg)
	} else if msg.Payload.MessageIs(&protobufs.ApiRuntimeEntityExistsRequest{}) {
		w.onRuntimeEntityExistsRequest(ctx, client, msg)
	}
}

func (w *RuntimeWorker) OnStoreConnected(context.Context) {
	w.isStoreConnected = true
}

func (w *RuntimeWorker) OnStoreDisconnected() {
	w.isStoreConnected = false
}

func (w *RuntimeWorker) onRuntimeDatabaseRequest(ctx context.Context, client web.Client, msg web.Message) {
	req := new(protobufs.ApiRuntimeDatabaseRequest)
	rsp := new(protobufs.ApiRuntimeDatabaseResponse)

	if err := msg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	reqs := []data.Request{}
	for _, r := range req.Requests {
		reqs = append(reqs, request.FromPb(r))
	}

	if req.RequestType == protobufs.ApiRuntimeDatabaseRequest_READ {
		log.Info("Read request: %v", req.Requests)
		w.store.Read(ctx, reqs...)
		rsp.Response = req.Requests
	} else if req.RequestType == protobufs.ApiRuntimeDatabaseRequest_WRITE {
		log.Info("Write request: %v", req.Requests)
		w.store.Write(ctx, reqs...)
		rsp.Response = req.Requests
	} else {
		log.Error("Could not handle request %v. Unknown request type.", req)
	}

	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(rsp); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	client.Write(msg)
}

func (w *RuntimeWorker) onRuntimeRegisterNotificationRequest(ctx context.Context, client web.Client, msg web.Message) {
	req := new(protobufs.ApiRuntimeRegisterNotificationRequest)
	rsp := new(protobufs.ApiRuntimeRegisterNotificationResponse)

	if err := msg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	if w.clientNotificationQueue[client.Id()] == nil {
		log.Warn("Client %s has no notification queue. Is it likely that it has just diconnected?", client.Id())
		return
	}

	for _, cfg := range req.Requests {
		token := w.store.Notify(ctx, notification.FromConfigPb(cfg), notification.NewCallback(func(ctx context.Context, n data.Notification) {
			if w.clientNotificationQueue[client.Id()][n.GetToken()] != nil {
				w.clientNotificationQueue[client.Id()][n.GetToken()] = append(w.clientNotificationQueue[client.Id()][n.GetToken()], n)
			}
		}))

		if w.clientNotificationTokens[client.Id()][token.Id()] != nil {
			w.clientNotificationTokens[client.Id()][token.Id()].Unbind(ctx)
		}
		w.clientNotificationTokens[client.Id()][token.Id()] = token

		if w.clientNotificationQueue[client.Id()][token.Id()] == nil {
			w.clientNotificationQueue[client.Id()][token.Id()] = make([]data.Notification, 0)
		}

		log.Info("Registered notification token '%v' for client %s", token.Id(), client.Id())

		rsp.Tokens = append(rsp.Tokens, token.Id())
	}

	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(rsp); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	client.Write(msg)
}

func (w *RuntimeWorker) onRuntimeUnregisterNotificationRequest(ctx context.Context, client web.Client, msg web.Message) {
	req := new(protobufs.ApiRuntimeUnregisterNotificationRequest)
	rsp := new(protobufs.ApiRuntimeUnregisterNotificationResponse)

	if err := msg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	for _, token := range req.Tokens {
		if w.clientNotificationTokens[client.Id()][token] != nil {
			w.clientNotificationTokens[client.Id()][token].Unbind(ctx)
			delete(w.clientNotificationTokens[client.Id()], token)
		}

		if w.clientNotificationQueue[client.Id()][token] != nil {
			delete(w.clientNotificationQueue[client.Id()], token)
		}

		log.Info("Unregistered notification: %v for client %s", token, client.Id())
	}

	rsp.Status = protobufs.ApiRuntimeUnregisterNotificationResponse_SUCCESS
	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(rsp); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	client.Write(msg)
}

func (w *RuntimeWorker) onRuntimeGetNotificationsRequest(_ context.Context, client web.Client, msg web.Message) {
	req := new(protobufs.ApiRuntimeGetNotificationsRequest)
	rsp := new(protobufs.ApiRuntimeGetNotificationsResponse)

	if err := msg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	for tok, ntfs := range w.clientNotificationQueue[client.Id()] {
		for _, n := range ntfs {
			rsp.Notifications = append(rsp.Notifications, notification.ToPb(n))
		}
		w.clientNotificationQueue[client.Id()][tok] = make([]data.Notification, 0)
	}

	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(rsp); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	client.Write(msg)
}

func (w *RuntimeWorker) onRuntimeGetDatabaseConnectionStatusRequest(_ context.Context, client web.Client, msg web.Message) {
	req := new(protobufs.ApiRuntimeGetDatabaseConnectionStatusRequest)
	rsp := new(protobufs.ApiRuntimeGetDatabaseConnectionStatusResponse)

	if err := msg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	rsp.Connected = w.isStoreConnected

	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(rsp); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	client.Write(msg)
}

func (w *RuntimeWorker) onRuntimeGetEntitiesRequest(ctx context.Context, client web.Client, msg web.Message) {
	req := new(protobufs.ApiRuntimeGetEntitiesRequest)
	rsp := new(protobufs.ApiRuntimeGetEntitiesResponse)

	if err := msg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	entities := query.New(w.store).
		Select().
		From(req.EntityType).
		Execute(ctx)

	for _, ent := range entities {
		rsp.Entities = append(rsp.Entities, entity.ToEntityPb(ent))
	}

	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(rsp); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	client.Write(msg)
}

func (w *RuntimeWorker) onRuntimeFieldExistsRequest(ctx context.Context, client web.Client, msg web.Message) {
	req := new(protobufs.ApiRuntimeFieldExistsRequest)
	rsp := new(protobufs.ApiRuntimeFieldExistsResponse)

	if err := msg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	rsp.Exists = w.store.FieldExists(ctx, req.FieldName, req.EntityType)

	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(rsp); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	client.Write(msg)
}

func (w *RuntimeWorker) onRuntimeEntityExistsRequest(ctx context.Context, client web.Client, msg web.Message) {
	req := new(protobufs.ApiRuntimeEntityExistsRequest)
	rsp := new(protobufs.ApiRuntimeEntityExistsResponse)

	if err := msg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		msg.Header.Timestamp = timestamppb.Now()
		if err := msg.Payload.MarshalFrom(rsp); err != nil {
			log.Error("Could not marshal response: %v", err)
			return
		}

		client.Write(msg)
		return
	}

	rsp.Exists = w.store.EntityExists(ctx, req.EntityId)

	msg.Header.Timestamp = timestamppb.Now()
	if err := msg.Payload.MarshalFrom(rsp); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	client.Write(msg)
}
