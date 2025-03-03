package main

import (
	"context"
	"time"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/query"
	"github.com/rqure/qlib/pkg/data/request"
	qnats "github.com/rqure/qlib/pkg/data/store/nats"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
)

const LeaseDuration = 1 * time.Minute

type NotificationLease struct {
	Config   data.NotificationConfig
	ExpireAt time.Time
}

type NotificationManager interface {
	data.ModifiableNotificationPublisher

	Register(data.NotificationConfig)
	Unregister(data.NotificationConfig)

	ClearExpired()
}

type notificationManager struct {
	core          qnats.Core
	entityManager data.EntityManager
	fieldOperator data.FieldOperator

	registeredNotifications map[string]map[string]NotificationLease
}

func NewNotificationManager(core qnats.Core) NotificationManager {
	return &notificationManager{
		core:                    core,
		registeredNotifications: make(map[string]map[string]NotificationLease),
	}
}

func (p *notificationManager) SetEntityManager(em data.EntityManager) {
	p.entityManager = em
}

func (p *notificationManager) SetFieldOperator(fo data.FieldOperator) {
	p.fieldOperator = fo
}

func (p *notificationManager) PublishNotifications(ctx context.Context, curr data.Request, prev data.Request) {
	// Failed to read old value (it may not exist initially)
	if !prev.IsSuccessful() {
		log.Trace("Failed to read old value: %v", prev)
		return
	}

	changed := !proto.Equal(field.ToAnyPb(curr.GetValue()), field.ToAnyPb(prev.GetValue()))

	resolver := query.NewIndirectionResolver(p.entityManager, p.fieldOperator)
	indirectField, indirectEntity := resolver.Resolve(ctx, curr.GetEntityId(), curr.GetFieldName())

	if indirectField == "" || indirectEntity == "" {
		log.Error("Failed to resolve indirection: %v", curr)
		return
	}

	for _, lease := range p.registeredNotifications[curr.GetEntityId()] {
		cfg := lease.Config
		if cfg.GetNotifyOnChange() && !changed {
			continue
		}

		notifMsg := &protobufs.DatabaseNotification{
			Token:    cfg.GetToken(),
			Current:  field.ToFieldPb(field.FromRequest(curr)),
			Previous: field.ToFieldPb(field.FromRequest(prev)),
			Context:  []*protobufs.DatabaseField{},
		}

		for _, ctxField := range cfg.GetContextFields() {
			ctxReq := request.New().SetEntityId(indirectEntity).SetFieldName(ctxField)
			p.fieldOperator.Read(ctx, ctxReq)
			if ctxReq.IsSuccessful() {
				notifMsg.Context = append(notifMsg.Context, field.ToFieldPb(field.FromRequest(ctxReq)))
			}
		}

		// Choose the appropriate subject based on distribution setting
		if cfg.IsDistributed() {
			// For distributed notifications, use a queue subject
			// This ensures only one subscriber receives the notification
			p.core.Publish(p.core.GetKeyGenerator().GetDistributedNotificationGroupSubject(cfg.GetServiceId()), notifMsg)
		} else {
			// For non-distributed notifications, use the regular subject
			// All subscribers will receive the notification
			p.core.Publish(p.core.GetKeyGenerator().GetNotificationGroupSubject(cfg.GetServiceId()), notifMsg)
		}
	}

	fetchedEntity := p.entityManager.GetEntity(ctx, indirectEntity)
	if fetchedEntity == nil {
		log.Error("Failed to get entity: %v (indirect=%v)", curr.GetEntityId(), indirectEntity)
		return
	}

	for _, lease := range p.registeredNotifications[fetchedEntity.GetType()] {
		cfg := lease.Config

		if cfg.GetNotifyOnChange() && !changed {
			continue
		}

		notifMsg := &protobufs.DatabaseNotification{
			Token:    cfg.GetToken(),
			Current:  field.ToFieldPb(field.FromRequest(curr)),
			Previous: field.ToFieldPb(field.FromRequest(prev)),
			Context:  []*protobufs.DatabaseField{},
		}

		for _, ctxField := range cfg.GetContextFields() {
			ctxReq := request.New().SetEntityId(indirectEntity).SetFieldName(ctxField)
			p.fieldOperator.Read(ctx, ctxReq)
			if ctxReq.IsSuccessful() {
				notifMsg.Context = append(notifMsg.Context, field.ToFieldPb(field.FromRequest(ctxReq)))
			}
		}

		// Choose the appropriate subject based on distribution setting
		if cfg.IsDistributed() {
			// For distributed notifications, use a queue subject
			// This ensures only one subscriber receives the notification
			p.core.Publish(p.core.GetKeyGenerator().GetDistributedNotificationGroupSubject(cfg.GetServiceId()), notifMsg)
		} else {
			// For non-distributed notifications, use the regular subject
			// All subscribers will receive the notification
			p.core.Publish(p.core.GetKeyGenerator().GetNotificationGroupSubject(cfg.GetServiceId()), notifMsg)
		}
	}
}

func (p *notificationManager) Register(cfg data.NotificationConfig) {
	lease := NotificationLease{
		Config:   cfg,
		ExpireAt: time.Now().Add(LeaseDuration),
	}

	if p.registeredNotifications[cfg.GetEntityId()] == nil {
		p.registeredNotifications[cfg.GetEntityId()] = make(map[string]NotificationLease)
	}

	p.registeredNotifications[cfg.GetEntityId()][cfg.GetToken()] = lease
}

func (p *notificationManager) Unregister(cfg data.NotificationConfig) {
	if p.registeredNotifications[cfg.GetEntityId()] == nil {
		return
	}

	delete(p.registeredNotifications[cfg.GetEntityId()], cfg.GetToken())

	if len(p.registeredNotifications[cfg.GetEntityId()]) == 0 {
		delete(p.registeredNotifications, cfg.GetEntityId())
	}
}

func (p *notificationManager) ClearExpired() {
	activeLeases := make(map[string]map[string]NotificationLease)

	for entityId, leases := range p.registeredNotifications {
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

	p.registeredNotifications = activeLeases
}
