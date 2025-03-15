package main

import (
	"context"
	"time"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qfield"
	"github.com/rqure/qlib/pkg/qdata/qquery"
	"github.com/rqure/qlib/pkg/qdata/qrequest"
	"github.com/rqure/qlib/pkg/qdata/qstore/qnats"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
)

const LeaseDuration = 1 * time.Minute

type NotificationLease struct {
	Config   qdata.NotificationConfig
	ExpireAt time.Time
}

type NotificationManager interface {
	qdata.ModifiableNotificationPublisher

	Register(qdata.NotificationConfig)
	Unregister(qdata.NotificationConfig)

	ClearExpired()
}

type notificationManager struct {
	core          qnats.Core
	entityManager qdata.EntityManager
	fieldOperator qdata.FieldOperator

	registeredNotifications map[string]map[string]NotificationLease
}

func NewNotificationManager(core qnats.Core) NotificationManager {
	return &notificationManager{
		core:                    core,
		registeredNotifications: make(map[string]map[string]NotificationLease),
	}
}

func (me *notificationManager) SetEntityManager(em qdata.EntityManager) {
	me.entityManager = em
}

func (me *notificationManager) SetFieldOperator(fo qdata.FieldOperator) {
	me.fieldOperator = fo
}

func (me *notificationManager) PublishNotifications(ctx context.Context, curr qdata.Request, prev qdata.Request) {
	// Failed to read old value (it may not exist initially)
	if !prev.IsSuccessful() {
		qlog.Trace("Failed to read old value: %v", prev)
		return
	}

	changed := !proto.Equal(qfield.ToAnyPb(curr.GetValue()), qfield.ToAnyPb(prev.GetValue()))

	resolver := qquery.NewIndirectionResolver(me.entityManager, me.fieldOperator)
	indirectEntity, indirectField := resolver.Resolve(ctx, curr.GetEntityId(), curr.GetFieldName())

	if indirectField == "" || indirectEntity == "" {
		qlog.Error("Failed to resolve indirection: %v", curr)
		return
	}

	for _, lease := range me.registeredNotifications[curr.GetEntityId()] {
		cfg := lease.Config
		if cfg.GetNotifyOnChange() && !changed {
			continue
		}

		notifMsg := &qprotobufs.DatabaseNotification{
			Token:    cfg.GetToken(),
			Current:  qfield.ToFieldPb(qfield.FromRequest(curr)),
			Previous: qfield.ToFieldPb(qfield.FromRequest(prev)),
			Context:  []*qprotobufs.DatabaseField{},
		}

		for _, ctxField := range cfg.GetContextFields() {
			ctxReq := qrequest.New().SetEntityId(indirectEntity).SetFieldName(ctxField)
			me.fieldOperator.Read(ctx, ctxReq)
			if ctxReq.IsSuccessful() {
				notifMsg.Context = append(notifMsg.Context, qfield.ToFieldPb(qfield.FromRequest(ctxReq)))
			}
		}

		// Choose the appropriate subject based on distribution setting
		if cfg.IsDistributed() {
			// For distributed notifications, use a queue subject
			// This ensures only one subscriber receives the notification
			me.core.Publish(me.core.GetKeyGenerator().GetDistributedNotificationGroupSubject(cfg.GetServiceId()), notifMsg)
		} else {
			// For non-distributed notifications, use the regular subject
			// All subscribers will receive the notification
			me.core.Publish(me.core.GetKeyGenerator().GetNotificationGroupSubject(cfg.GetServiceId()), notifMsg)
		}
	}

	fetchedEntity := me.entityManager.GetEntity(ctx, indirectEntity)
	if fetchedEntity == nil {
		qlog.Error("Failed to get entity: %v (indirect=%v)", curr.GetEntityId(), indirectEntity)
		return
	}

	for _, lease := range me.registeredNotifications[fetchedEntity.GetType()] {
		cfg := lease.Config

		if cfg.GetNotifyOnChange() && !changed {
			continue
		}

		notifMsg := &qprotobufs.DatabaseNotification{
			Token:    cfg.GetToken(),
			Current:  qfield.ToFieldPb(qfield.FromRequest(curr)),
			Previous: qfield.ToFieldPb(qfield.FromRequest(prev)),
			Context:  []*qprotobufs.DatabaseField{},
		}

		for _, ctxField := range cfg.GetContextFields() {
			ctxReq := qrequest.New().SetEntityId(indirectEntity).SetFieldName(ctxField)
			me.fieldOperator.Read(ctx, ctxReq)
			if ctxReq.IsSuccessful() {
				notifMsg.Context = append(notifMsg.Context, qfield.ToFieldPb(qfield.FromRequest(ctxReq)))
			}
		}

		// Choose the appropriate subject based on distribution setting
		if cfg.IsDistributed() {
			// For distributed notifications, use a queue subject
			// This ensures only one subscriber receives the notification
			me.core.Publish(me.core.GetKeyGenerator().GetDistributedNotificationGroupSubject(cfg.GetServiceId()), notifMsg)
		} else {
			// For non-distributed notifications, use the regular subject
			// All subscribers will receive the notification
			me.core.Publish(me.core.GetKeyGenerator().GetNotificationGroupSubject(cfg.GetServiceId()), notifMsg)
		}
	}
}

func (me *notificationManager) Register(cfg qdata.NotificationConfig) {
	lease := NotificationLease{
		Config:   cfg,
		ExpireAt: time.Now().Add(LeaseDuration),
	}

	if me.registeredNotifications[cfg.GetEntityId()] == nil {
		me.registeredNotifications[cfg.GetEntityId()] = make(map[string]NotificationLease)
	}

	me.registeredNotifications[cfg.GetEntityId()][cfg.GetToken()] = lease
}

func (me *notificationManager) Unregister(cfg qdata.NotificationConfig) {
	if me.registeredNotifications[cfg.GetEntityId()] == nil {
		return
	}

	delete(me.registeredNotifications[cfg.GetEntityId()], cfg.GetToken())

	if len(me.registeredNotifications[cfg.GetEntityId()]) == 0 {
		delete(me.registeredNotifications, cfg.GetEntityId())
	}
}

func (me *notificationManager) ClearExpired() {
	activeLeases := make(map[string]map[string]NotificationLease)

	for entityId, leases := range me.registeredNotifications {
		for token, lease := range leases {
			if time.Now().After(lease.ExpireAt) {
				continue
			}

			if activeLeases[entityId] == nil {
				activeLeases[entityId] = make(map[string]NotificationLease)
			}

			activeLeases[entityId][token] = lease
		}
	}

	me.registeredNotifications = activeLeases
}
