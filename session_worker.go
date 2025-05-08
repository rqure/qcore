package main

import (
	"context"
	"fmt"
	"time"

	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qapp/qworkers"
	"github.com/rqure/qlib/pkg/qauthentication"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
)

const (
	initSyncInterval  = 1 * time.Minute
	fullSyncInterval  = 1 * time.Minute
	eventPollInterval = 1 * time.Second
)

type SessionReadyCriteria struct {
	isAuthReady bool
}

func (me *SessionReadyCriteria) IsReady() bool {
	return me.isAuthReady
}

func (me *SessionReadyCriteria) OnAuthReady(qss.VoidType) {
	me.isAuthReady = true
}

func (me *SessionReadyCriteria) OnAuthNotReady(qss.VoidType) {
	me.isAuthReady = false
}

func NewSessionReadyCriteria(w SessionWorker) qworkers.ReadinessCriteria {
	c := &SessionReadyCriteria{
		isAuthReady: false,
	}

	w.AuthReady().Connect(c.OnAuthReady)
	w.AuthNotReady().Connect(c.OnAuthNotReady)

	return c
}

type SessionWorker interface {
	qapp.Worker

	AuthReady() qss.Signal[qss.VoidType]
	AuthNotReady() qss.Signal[qss.VoidType]

	OnReady(context.Context)
	OnNotReady(context.Context)
}

type sessionWorker struct {
	handle qcontext.Handle

	authReady        qss.Signal[qss.VoidType]
	authNotReady     qss.Signal[qss.VoidType]
	isAdminAuthReady bool

	store   *qdata.Store
	isReady bool

	core         qauthentication.Core
	admin        qauthentication.Admin
	eventEmitter qauthentication.EventEmitter

	initTimer      *time.Ticker
	fullSyncTimer  *time.Ticker
	eventPollTimer *time.Ticker
}

func NewSessionWorker(store *qdata.Store) SessionWorker {
	return &sessionWorker{
		store:        store,
		authReady:    qss.New[qss.VoidType](),
		authNotReady: qss.New[qss.VoidType](),
	}
}

func (me *sessionWorker) Init(ctx context.Context) {
	me.handle = qcontext.GetHandle(ctx)

	me.core = qauthentication.NewCore()
	me.admin = qauthentication.NewAdmin(me.core)
	me.eventEmitter = qauthentication.NewEventEmitter(me.core)
	me.eventEmitter.Signal().Connect(me.handleKeycloakEvent)

	me.initTimer = time.NewTicker(initSyncInterval)
	me.fullSyncTimer = time.NewTicker(fullSyncInterval)
	me.eventPollTimer = time.NewTicker(eventPollInterval)

	me.performInit(ctx)
}

func (me *sessionWorker) Deinit(context.Context) {
	me.initTimer.Stop()
	me.fullSyncTimer.Stop()
	me.eventPollTimer.Stop()
}

func (me *sessionWorker) DoWork(ctx context.Context) {
	startTime := time.Now()
	defer func() {
		qlog.Trace("Took %s to process", time.Since(startTime))
	}()

	select {
	case <-me.initTimer.C:
		me.performInit(ctx)
	default:
		break
	}

	if !me.isReady {
		return
	}

	select {
	case <-me.fullSyncTimer.C:
		err := me.performFullSync(ctx)
		if err != nil {
			qlog.Warn("Failed to perform full sync: %v", err)
		} else {
			qlog.Trace("Performed full sync")
		}
	default:
		break
	}

	select {
	case <-me.eventPollTimer.C:
		err := me.admin.ProcessEvents(ctx, me.eventEmitter)
		if err != nil {
			qlog.Warn("Failed to process all new session events: %v", err)
		} else {
			qlog.Trace("Processed new session events")
		}
	default:
		break
	}

}

func (me *sessionWorker) performInit(ctx context.Context) {
	qlog.Info("Ensuring setup of auth database...")
	err := me.admin.EnsureSetup(ctx)
	if err != nil {
		qlog.Warn("Failed to ensure setup: %v", err)
		me.setAuthReadiness(false, fmt.Sprintf("failed to ensure setup: %v", err))
		return
	}

	qlog.Info("Setup of auth database complete")
	me.setAuthReadiness(true, "")

	err = me.performFullSync(ctx)
	if err != nil {
		qlog.Warn("Failed to perform initial sync: %v", err)
	} else {
		qlog.Trace("Performed initial sync of users")
	}
}

func (me *sessionWorker) OnReady(ctx context.Context) {
	me.isReady = true

	iter, err := me.store.PrepareQuery(`SELECT "$EntityId", LastEventTime FROM SessionController`)
	if err != nil {
		qlog.Warn("Failed to prepare query: %v", err)
		return
	}

	iter.ForEach(ctx, func(row qdata.QueryRow) bool {
		sessionController := row.AsEntity()
		lastEventTime := sessionController.Field(qdata.FTLastEventTime).Value.GetTimestamp()
		me.eventEmitter.SetLastEventTime(lastEventTime)
		return true
	})
}

func (me *sessionWorker) OnNotReady(context.Context) {
	me.isReady = false
}

func (me *sessionWorker) handleKeycloakEvent(e qauthentication.EmittedEvent) {
	toString := func(e qauthentication.Event) string {
		return fmt.Sprintf("[%s] type=%s realm=%s client=%s user=%s session=%s ip=%s error=%q details=%v",
			e.Time().Format(time.RFC3339),
			e.Type(),
			e.RealmID(),
			e.ClientID(),
			e.UserID(),
			e.SessionID(),
			e.IPAddress(),
			e.Error(),
			e.Details())
	}

	qlog.Trace("Received Keycloak event: %s", toString(e.Event))

	iter, err := me.store.PrepareQuery(`SELECT "$EntityId", LastEventTime FROM SessionController`)
	if err != nil {
		qlog.Warn("Failed to prepare query: %v", err)
		return
	}

	reqs := []*qdata.Request{}
	iter.ForEach(e.Ctx, func(row qdata.QueryRow) bool {
		sessionController := row.AsEntity()
		sessionController.Field(qdata.FTLastEventTime).Value.SetTimestamp(time.Now())
		reqs = append(reqs, sessionController.Field(qdata.FTLastEventTime).AsWriteRequest())
		return true
	})

	err = me.store.Write(e.Ctx, reqs...)
	if err != nil {
		qlog.Warn("Failed to write last event time: %v", err)
		return
	}
}

func (me *sessionWorker) performFullSync(ctx context.Context) error {
	usersFolder, err := qdata.NewPathResolver(me.store).Resolve(ctx, "Root", "Security Models", "Users")
	if err != nil {
		return fmt.Errorf("failed to resolve users folder: %w", err)
	}

	keycloakUsersByName, err := me.admin.GetUsers(ctx)
	if err != nil {
		return fmt.Errorf("failed to get Keycloak users: %w", err)
	}

	SourceOfTruth_QOS := 0
	SourceOfTruth_Keycloak := 1
	iter, err := me.store.PrepareQuery(`SELECT Name, SourceOfTruth, Parent FROM User WHERE Parent = %q`, usersFolder.EntityId)
	if err != nil {
		return fmt.Errorf("failed to prepare query: %w", err)
	}

	// 1. Sync store users to Keycloak
	storeUsersByName := make(map[string]*qdata.Entity)
	iter.ForEach(ctx, func(row qdata.QueryRow) bool {
		user := row.AsEntity()
		name := user.Field(qdata.FTName).Value.GetString()
		sourceOfTruth := user.Field("SourceOfTruth").Value.GetChoice()

		storeUsersByName[name] = user

		if sourceOfTruth != SourceOfTruth_QOS {
			return true // continue to next user
		}

		// Create user in Keycloak if it doesn't exist
		if _, ok := keycloakUsersByName[name]; !ok {
			qlog.Info("Creating QOS user '%s' in Keycloak", name)
			if err := me.admin.CreateUser(ctx, name, name); err != nil {
				qlog.Error("Failed to sync user %s to Keycloak: %v", name, err)
			}
		}

		return true // continue to next user
	})

	// 2. Sync Keycloak users to store
	reqs := []*qdata.Request{}
	for _, kcUser := range keycloakUsersByName {
		user, ok := storeUsersByName[kcUser.GetUsername()]
		if !ok {
			qlog.Info("Creating QOS user '%s' from Keycloak", kcUser.GetUsername())
			user, err = me.store.CreateEntity(ctx, qdata.ETUser, usersFolder.EntityId, kcUser.GetUsername())
			if err != nil {
				qlog.Error("Failed to create Keycloak user %q in QOS: %v", kcUser.GetUsername(), err)
				continue
			}

			user.Field(qdata.FTSourceOfTruth).Value.FromChoice(SourceOfTruth_Keycloak)
			user.Field(qdata.FTKeycloakId).Value.FromString(kcUser.GetID())
			user.Field(qdata.FTEmail).Value.FromString(kcUser.GetEmail())
			user.Field(qdata.FTFirstName).Value.FromString(kcUser.GetFirstName())
			user.Field(qdata.FTLastName).Value.FromString(kcUser.GetLastName())
			user.Field(qdata.FTIsEmailVerified).Value.FromBool(kcUser.IsEmailVerified())
			user.Field(qdata.FTIsEnabled).Value.FromBool(kcUser.IsEnabled())
			user.Field(qdata.FTJSON).Value.FromString(kcUser.JSON())

			reqs = append(reqs,
				user.Field(qdata.FTSourceOfTruth).AsWriteRequest())
		} else {
			qlog.Info("Updating QOS user '%s' from Keycloak", kcUser.GetUsername())
			user.Field(qdata.FTKeycloakId).Value.FromString(kcUser.GetID())
			user.Field(qdata.FTEmail).Value.FromString(kcUser.GetEmail())
			user.Field(qdata.FTFirstName).Value.FromString(kcUser.GetFirstName())
			user.Field(qdata.FTLastName).Value.FromString(kcUser.GetLastName())
			user.Field(qdata.FTIsEmailVerified).Value.FromBool(kcUser.IsEmailVerified())
			user.Field(qdata.FTIsEnabled).Value.FromBool(kcUser.IsEnabled())
			user.Field(qdata.FTJSON).Value.FromString(kcUser.JSON())
		}

		reqs = append(reqs,
			user.Field(qdata.FTKeycloakId).AsWriteRequest(),
			user.Field(qdata.FTEmail).AsWriteRequest(),
			user.Field(qdata.FTFirstName).AsWriteRequest(),
			user.Field(qdata.FTLastName).AsWriteRequest(),
			user.Field(qdata.FTIsEmailVerified).AsWriteRequest(),
			user.Field(qdata.FTIsEnabled).AsWriteRequest(),
			user.Field(qdata.FTJSON).AsWriteRequest())
	}

	if len(reqs) > 0 {
		if err := me.store.Write(ctx, reqs...); err != nil {
			return fmt.Errorf("failed to write users to store: %w", err)
		}
	}

	return nil
}

func (me *sessionWorker) setAuthReadiness(ready bool, reason string) {
	if me.isAdminAuthReady == ready {
		return
	}

	me.isAdminAuthReady = ready

	if ready {
		qlog.Info("Authentication status changed to [READY]")
		me.authReady.Emit(qss.Void)
	} else {
		qlog.Warn("Authentication status changed to [NOT READY] with reason: %s", reason)
		me.authNotReady.Emit(qss.Void)
	}
}

func (me *sessionWorker) AuthReady() qss.Signal[qss.VoidType] {
	return me.authReady
}

func (me *sessionWorker) AuthNotReady() qss.Signal[qss.VoidType] {
	return me.authNotReady
}
