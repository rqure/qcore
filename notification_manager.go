package main

import (
	"context"
	"encoding/base64"

	"github.com/redis/go-redis/v9"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/data/request"
	qnats "github.com/rqure/qlib/pkg/data/store/nats"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
)

type NotificationManager interface {
	data.ModifiableNotificationPublisher
}

type NotificationPublisher struct {
	core          qnats.Core
	entityManager data.EntityManager
	fieldOperator data.FieldOperator
}

func NewNotificationPublisher(core qnats.Core) data.ModifiableNotificationPublisher {
	return &NotificationPublisher{core: core}
}

func (p *NotificationPublisher) SetEntityManager(em data.EntityManager) {
	p.entityManager = em
}

func (p *NotificationPublisher) SetFieldOperator(fo data.FieldOperator) {
	p.fieldOperator = fo
}

func (p *NotificationPublisher) PublishNotifications(ctx context.Context, curr data.Request, prev data.Request) {
	// Failed to read old value (it may not exist initially)
	if !prev.IsSuccessful() {
		log.Trace("Failed to read old value: %v", prev)
		return
	}

	changed := !proto.Equal(field.ToAnyPb(curr.GetValue()), field.ToAnyPb(prev.GetValue()))

	indirectField, indirectEntity := p.fieldOperator.ResolveIndirection(ctx, curr.GetFieldName(), curr.GetEntityId())

	if indirectField == "" || indirectEntity == "" {
		log.Error("Failed to resolve indirection: %v", curr)
		return
	}

	m, err := s.client.SMembers(ctx, s.keygen.GetEntityIdNotificationConfigKey(indirectEntity, indirectField)).Result()
	if err != nil {
		log.Error("Failed to get notification config: %v", err)
		return
	}

	for _, e := range m {
		b, err := base64.StdEncoding.DecodeString(e)
		if err != nil {
			log.Error("Failed to decode notification config: %v", err)
			continue
		}

		p := &protobufs.DatabaseNotificationConfig{}
		err = proto.Unmarshal(b, p)
		if err != nil {
			log.Error("Failed to unmarshal notification config: %v", err)
			continue
		}
		nc := notification.FromConfigPb(p)

		if nc.GetNotifyOnChange() && !changed {
			continue
		}

		n := &protobufs.DatabaseNotification{
			Token:    e,
			Current:  field.ToFieldPb(field.FromRequest(curr)),
			Previous: field.ToFieldPb(field.FromRequest(prev)),
			Context:  []*protobufs.DatabaseField{},
		}

		for _, cf := range nc.GetContextFields() {
			cr := request.New().SetEntityId(indirectEntity).SetFieldName(cf)
			s.Read(ctx, cr)
			if cr.IsSuccessful() {
				n.Context = append(n.Context, field.ToFieldPb(field.FromRequest(cr)))
			}
		}

		b, err = proto.Marshal(n)
		if err != nil {
			log.Error("Failed to marshal notification: %v", err)
			continue
		}

		_, err = s.client.XAdd(ctx, &redis.XAddArgs{
			Stream: s.keygen.GetNotificationChannelKey(p.ServiceId),
			Values: []string{"data", base64.StdEncoding.EncodeToString(b)},
			MaxLen: MaxStreamLength,
			Approx: true,
		}).Result()
		if err != nil {
			log.Error("Failed to add notification: %v", err)
			continue
		}
	}

	fetchedEntity := s.GetEntity(ctx, indirectEntity)
	if fetchedEntity == nil {
		log.Error("Failed to get entity: %v (indirect=%v)", curr.GetEntityId(), indirectEntity)
		return
	}

	m, err = s.client.SMembers(ctx, s.keygen.GetEntityTypeNotificationConfigKey(fetchedEntity.GetType(), indirectField)).Result()
	if err != nil {
		log.Error("Failed to get notification config: %v", err)
		return
	}

	for _, e := range m {
		b, err := base64.StdEncoding.DecodeString(e)
		if err != nil {
			log.Error("Failed to decode notification config: %v", err)
			continue
		}

		p := &protobufs.DatabaseNotificationConfig{}
		err = proto.Unmarshal(b, p)
		if err != nil {
			log.Error("Failed to unmarshal notification config: %v", err)
			continue
		}

		nc := notification.FromConfigPb(p)
		if nc.GetNotifyOnChange() && !changed {
			continue
		}

		n := &protobufs.DatabaseNotification{
			Token:    e,
			Current:  field.ToFieldPb(field.FromRequest(curr)),
			Previous: field.ToFieldPb(field.FromRequest(prev)),
			Context:  []*protobufs.DatabaseField{},
		}

		for _, cf := range nc.GetContextFields() {
			cr := request.New().SetEntityId(indirectEntity).SetFieldName(cf)
			s.Read(ctx, cr)
			if cr.IsSuccessful() {
				n.Context = append(n.Context, field.ToFieldPb(field.FromRequest(cr)))
			}
		}

		b, err = proto.Marshal(n)
		if err != nil {
			log.Error("Failed to marshal notification: %v", err)
			continue
		}

		_, err = s.client.XAdd(ctx, &redis.XAddArgs{
			Stream: s.keygen.GetNotificationChannelKey(p.ServiceId),
			Values: []string{"data", base64.StdEncoding.EncodeToString(b)},
			MaxLen: MaxStreamLength,
			Approx: true,
		}).Result()
		if err != nil {
			log.Error("Failed to add notification: %v", err)
			continue
		}
	}
}
