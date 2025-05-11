package main

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/rqure/qlib/pkg/qapp/qworkers"
	"github.com/rqure/qlib/pkg/qauthentication"
	"github.com/rqure/qlib/pkg/qauthorization"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
)

type SubjectManager interface {
	GetEntityId(ctx context.Context, subjectName string) (qdata.EntityId, error)

	OnReady(ctx context.Context)
	OnEntityCreated(args qworkers.EntityCreateArgs)
	OnEntityDeleted(args qworkers.EntityDeletedArgs)

	Verify(ctx context.Context, accessToken string) (context.Context, bool)
}

type subjectManager struct {
	store    qdata.StoreInteractor
	subjects map[string]qdata.EntityId
	rwMu     *sync.RWMutex
}

func NewSubjectManager(store qdata.StoreInteractor) SubjectManager {
	return &subjectManager{
		store:    store,
		subjects: make(map[string]qdata.EntityId),
		rwMu:     &sync.RWMutex{},
	}
}

func (me *subjectManager) GetEntityId(ctx context.Context, subjectName string) (qdata.EntityId, error) {
	me.rwMu.RLock()
	defer me.rwMu.RUnlock()
	if id, ok := me.subjects[subjectName]; ok {
		return id, nil
	}

	return "", fmt.Errorf("subject '%s' not found", subjectName)
}

func (me *subjectManager) OnReady(ctx context.Context) {
	me.rwMu.Lock()
	defer me.rwMu.Unlock()

	for _, eType := range []qdata.EntityType{qdata.ETUser, qdata.ETClient} {
		subjects, err := me.store.Find(ctx, eType, []qdata.FieldType{qdata.FTName})

		if err != nil {
			qlog.Error("Failed to load subjects of type '%s': %v", eType, err)
			continue
		}

		for _, subject := range subjects {
			name := subject.Field(qdata.FTName).Value.GetString()
			me.subjects[name] = subject.EntityId
		}
	}
}

func (me *subjectManager) OnEntityCreated(args qworkers.EntityCreateArgs) {
	req := new(qdata.Request).Init(args.Entity.EntityId, qdata.FTName)
	if err := me.store.Read(args.Ctx, req); err != nil {
		qlog.Error("Failed to read entity '%s': %v", args.Entity.EntityId, err)
		return
	}

	me.rwMu.Lock()
	defer me.rwMu.Unlock()
	me.subjects[req.Value.GetString()] = args.Entity.EntityId
}

func (me *subjectManager) OnEntityDeleted(args qworkers.EntityDeletedArgs) {
	if !slices.Contains([]qdata.EntityType{qdata.ETClient, qdata.ETUser}, args.Entity.EntityType) {
		return
	}

	me.rwMu.Lock()
	defer me.rwMu.Unlock()
	for name, entityId := range me.subjects {
		if entityId == args.Entity.EntityId {
			delete(me.subjects, name)
			break
		}
	}
}

// Verify is a helper method that verifies the user's authentication
// and returns a context with authorization if successful, or nil if authentication fails
func (me *subjectManager) Verify(ctx context.Context, accessToken string) (context.Context, bool) {
	clientProvider := qcontext.GetClientProvider[qauthentication.Client](ctx)
	client := clientProvider.Client(ctx)
	if client == nil {
		qlog.Warn("Client not found")
		return nil, false
	}

	subjectSession := client.AccessTokenToSession(ctx, accessToken)
	if !subjectSession.CheckIsValid(ctx) {
		qlog.Warn("Invalid session")
		return nil, false
	}

	subjectName, err := subjectSession.GetOwnerName(ctx)
	if err != nil {
		qlog.Warn("Could not get owner name: %v", err)
		return nil, false
	}

	entityId, err := me.GetEntityId(ctx, subjectName)
	if err != nil {
		if subjectName == "qinitdb" {
			qlog.Info("InitDB client detected, skipping authorization")
			return ctx, true // No special authorizer needed for qinitdb
		}

		qlog.Warn("Could not get entity ID: %v", err)
		return nil, false
	}

	authCtx := context.WithValue(
		ctx,
		qcontext.KeyAuthorizer,
		qauthorization.NewAuthorizer(entityId, me.store))

	return authCtx, true
}
