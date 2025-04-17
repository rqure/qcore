package main

import (
	"time"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qstore/qnats"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

const LeaseDuration = 1 * time.Minute

type NotificationLease struct {
	Config   qdata.NotificationConfig
	ExpireAt time.Time
}

type NotificationManager interface {
	Register(qdata.NotificationConfig)
	Unregister(qdata.NotificationConfig)

	ClearExpired()
}

type notificationManager struct {
	core qnats.NatsCore

	registeredNotifications map[string]map[string]NotificationLease

	store *qdata.Store
}

func NewNotificationManager(core qnats.NatsCore) NotificationManager {
	return &notificationManager{
		core:                    core,
		registeredNotifications: make(map[string]map[string]NotificationLease),
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

	leases := me.getEntityLeases(args.Curr.EntityId.AsString())

	for _, lease := range leases {
		cfg := lease.Config
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
		me.store.Read(args.Ctx, reqs...)
		for _, ctxReq := range reqs {
			if ctxReq.Success {
				notifMsg.Context = append(notifMsg.Context, ctxReq.AsField().AsFieldPb())
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

	fetchedEntity := new(qdata.Entity).Init(indirectEntity)
	typeLeases := me.getEntityLeases(fetchedEntity.EntityType.AsString())

	for _, lease := range typeLeases {
		cfg := lease.Config

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
		me.store.Read(args.Ctx, reqs...)
		for _, ctxReq := range reqs {
			if ctxReq.Success {
				notifMsg.Context = append(notifMsg.Context, ctxReq.AsField().AsFieldPb())
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

// Helper method to safely get entity leases
func (me *notificationManager) getEntityLeases(entityIdOrType string) []NotificationLease {
	if leases, ok := me.registeredNotifications[entityIdOrType]; ok {
		result := make([]NotificationLease, 0, len(leases))
		for _, lease := range leases {
			result = append(result, lease)
		}
		return result
	}
	return nil
}

func (me *notificationManager) Register(cfg qdata.NotificationConfig) {
	lease := NotificationLease{
		Config:   cfg,
		ExpireAt: time.Now().Add(LeaseDuration),
	}

	entityId := string(cfg.GetEntityId())
	if me.registeredNotifications[entityId] == nil {
		me.registeredNotifications[entityId] = make(map[string]NotificationLease)
	}

	me.registeredNotifications[entityId][cfg.GetToken()] = lease
}

func (me *notificationManager) Unregister(cfg qdata.NotificationConfig) {
	entityId := string(cfg.GetEntityId())
	if me.registeredNotifications[entityId] == nil {
		return
	}

	delete(me.registeredNotifications[entityId], cfg.GetToken())

	if len(me.registeredNotifications[entityId]) == 0 {
		delete(me.registeredNotifications, entityId)
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
