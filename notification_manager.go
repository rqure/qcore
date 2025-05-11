package main

import (
	"context"
	"sync"

	"github.com/coder/websocket"
	"github.com/rqure/qlib/pkg/qauthorization"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qnotify"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type NotificationManager interface {
	PublishNotifications(args qdata.PublishNotificationArgs)
	Register(conn *websocket.Conn, authorizer qauthorization.Authorizer, cfg qdata.NotificationConfig)
	Unregister(conn *websocket.Conn, cfg qdata.NotificationConfig)
	AddConn(conn *websocket.Conn)
	RemoveConn(conn *websocket.Conn)
}

type notificationManager struct {
	registeredNotifications map[*websocket.Conn]map[string]bool
	connAuthorizers         map[*websocket.Conn]qauthorization.Authorizer
	rwMu                    *sync.RWMutex
	store                   qdata.StoreInteractor
}

func NewNotificationManager(store qdata.StoreInteractor) NotificationManager {
	return &notificationManager{
		registeredNotifications: make(map[*websocket.Conn]map[string]bool),
		connAuthorizers:         make(map[*websocket.Conn]qauthorization.Authorizer),
		store:                   store,
		rwMu:                    &sync.RWMutex{},
	}
}

func (me *notificationManager) PublishNotifications(args qdata.PublishNotificationArgs) {
	// Failed to read old value (it may not exist initially)
	if !args.Prev.Success {
		qlog.Trace("Failed to read old value: %v", args.Prev)
		return
	}

	changed := args.Prev.Value != args.Curr.Value

	resolver := qdata.NewIndirectionResolver(me.store)
	indirectEntity, _, err := resolver.Resolve(args.Ctx, args.Curr.EntityId, args.Curr.FieldType)

	if err != nil {
		qlog.Error("Failed to resolve indirection: %v", args.Curr)
		return
	}

	me.rwMu.RLock()
	defer me.rwMu.RUnlock()
	for conn, tokens := range me.registeredNotifications {
		for token := range tokens {
			cfg := qnotify.FromToken(token)
			if cfg.GetEntityId() != args.Curr.EntityId {
				continue
			}

			if cfg.GetNotifyOnChange() && !changed {
				continue
			}

			notifMsg := &qprotobufs.DatabaseNotification{
				Token:    cfg.GetToken(),
				Current:  args.Curr.AsField().AsFieldPb(),
				Previous: args.Prev.AsField().AsFieldPb(),
				Context:  []*qprotobufs.DatabaseField{},
			}

			reqs := []*qdata.Request{}
			for _, ctxField := range cfg.GetContextFields() {
				reqs = append(reqs, new(qdata.Request).Init(indirectEntity, ctxField))
			}
			ctx := context.WithValue(args.Ctx, qcontext.KeyAuthorizer, me.connAuthorizers[conn])
			me.store.Read(ctx, reqs...)
			for _, ctxReq := range reqs {
				if ctxReq.Success {
					notifMsg.Context = append(notifMsg.Context, ctxReq.AsField().AsFieldPb())
				}
			}

			me.sendNotification(args.Ctx, conn, notifMsg)
		}
	}

	for conn, tokens := range me.registeredNotifications {
		for token := range tokens {
			cfg := qnotify.FromToken(token)
			if cfg.GetEntityType() != args.Curr.EntityId.GetEntityType() {
				continue
			}

			if cfg.GetNotifyOnChange() && !changed {
				continue
			}

			notifMsg := &qprotobufs.DatabaseNotification{
				Token:    cfg.GetToken(),
				Current:  args.Curr.AsField().AsFieldPb(),
				Previous: args.Prev.AsField().AsFieldPb(),
				Context:  []*qprotobufs.DatabaseField{},
			}

			reqs := []*qdata.Request{}
			for _, ctxField := range cfg.GetContextFields() {
				reqs = append(reqs, new(qdata.Request).Init(indirectEntity, ctxField))
			}
			ctx := context.WithValue(args.Ctx, qcontext.KeyAuthorizer, me.connAuthorizers[conn])
			me.store.Read(ctx, reqs...)
			for _, ctxReq := range reqs {
				if ctxReq.Success {
					notifMsg.Context = append(notifMsg.Context, ctxReq.AsField().AsFieldPb())
				}
			}

			me.sendNotification(args.Ctx, conn, notifMsg)
		}
	}
}

func (me *notificationManager) AddConn(conn *websocket.Conn) {
	me.rwMu.Lock()
	defer me.rwMu.Unlock()

	if _, ok := me.registeredNotifications[conn]; !ok {
		me.registeredNotifications[conn] = make(map[string]bool)
	}
}

func (me *notificationManager) RemoveConn(conn *websocket.Conn) {
	me.rwMu.Lock()
	defer me.rwMu.Unlock()

	delete(me.registeredNotifications, conn)
	delete(me.connAuthorizers, conn)
}

func (me *notificationManager) Register(conn *websocket.Conn, authorizer qauthorization.Authorizer, cfg qdata.NotificationConfig) {
	me.rwMu.Lock()
	defer me.rwMu.Unlock()

	if _, ok := me.registeredNotifications[conn]; !ok {
		me.registeredNotifications[conn] = make(map[string]bool)
	}

	me.connAuthorizers[conn] = authorizer
	me.registeredNotifications[conn][cfg.GetToken()] = true
}

func (me *notificationManager) Unregister(conn *websocket.Conn, cfg qdata.NotificationConfig) {
	me.rwMu.Lock()
	defer me.rwMu.Unlock()

	if _, ok := me.registeredNotifications[conn]; ok {
		delete(me.registeredNotifications[conn], cfg.GetToken())
		if len(me.registeredNotifications[conn]) == 0 {
			delete(me.registeredNotifications, conn)
		}
	}

	delete(me.connAuthorizers, conn)
}

func (me *notificationManager) sendNotification(ctx context.Context, conn *websocket.Conn, notifMsg *qprotobufs.DatabaseNotification) {
	anyMsg, err := anypb.New(notifMsg)
	if err != nil {
		qlog.Warn("Could not marshal notification message: %v", err)
		return
	}

	msg := &qprotobufs.ApiMessage{
		Header: &qprotobufs.ApiHeader{
			Timestamp: timestamppb.Now(),
		},
		Payload: anyMsg,
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		qlog.Warn("Could not marshal message: %v", err)
		return
	}

	if err := conn.Write(ctx, websocket.MessageBinary, data); err != nil {
		qlog.Warn("Could not send notification: %v", err)
	}

	qlog.Trace("Sent notification: %v", notifMsg)
}
