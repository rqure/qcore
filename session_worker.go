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

	modeManager ModeManager

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

func NewSessionWorker(store *qdata.Store, modeManager ModeManager) SessionWorker {
	return &sessionWorker{
		modeManager:  modeManager,
		store:        store,
		authReady:    qss.New[qss.VoidType](),
		authNotReady: qss.New[qss.VoidType](),
	}
}

func (me *sessionWorker) Init(ctx context.Context) {
	me.handle = qcontext.GetHandle(ctx)

	if !me.modeManager.HasModes(ModeWrite) {
		return
	}

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
	if !me.modeManager.HasModes(ModeWrite) {
		return
	}

	me.initTimer.Stop()
	me.fullSyncTimer.Stop()
	me.eventPollTimer.Stop()
}

func (me *sessionWorker) DoWork(ctx context.Context) {
	if !me.modeManager.HasModes(ModeWrite) {
		return
	}

	session := me.admin.Session(ctx)
	if session.IsValid(ctx) {
		if session.PastHalfLife(ctx) {
			err := session.Refresh(ctx)
			if err != nil {
				me.setAuthReadiness(false, fmt.Sprintf("failed to refresh session: %v", err))
				return
			}
		}
	} else {
		me.setAuthReadiness(false, "session is not valid")
		return
	}

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
		qlog.Trace("Performing full sync...")
		me.performFullSync(ctx)
		qlog.Trace("Full sync completed")
	default:
		break
	}

	select {
	case <-me.eventPollTimer.C:
		qlog.Trace("Processing new session events...")
		err := me.eventEmitter.ProcessNextBatch(ctx, session)
		if err != nil {
			qlog.Warn("Failed to process all new session events: %v", err)
		}
		qlog.Trace("Processing new session events completed")
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
	}

	qlog.Info("Setup of auth database complete")
	me.setAuthReadiness(true, "")
}

func (me *sessionWorker) OnReady(ctx context.Context) {
	me.isReady = true

	me.store.PrepareQuery("SELECT $EntityId, LastEventTime FROM SessionController").ForEach(ctx, func(row qdata.QueryRow) bool {
		sessionController := row.AsEntity()
		lastEventTime := sessionController.Field("LastEventTime").Value.GetTimestamp()
		me.eventEmitter.SetLastEventTime(lastEventTime)
		return true
	})
}

func (me *sessionWorker) OnNotReady(context.Context) {
	me.isReady = false
}

func (me *sessionWorker) handleKeycloakEvent(e qauthentication.EmittedEvent) {
	me.store.PrepareQuery("SELECT $EntityId, LastEventTime FROM SessionController").ForEach(e.Ctx, func(row qdata.QueryRow) bool {
		sessionController := row.AsEntity()
		sessionController.Field("LastEventTime").Value.SetTimestamp(time.Now())
		me.store.Write(e.Ctx, sessionController.Field("LastEventTime").AsWriteRequest())
		return true
	})
}

func (me *sessionWorker) performFullSync(ctx context.Context) error {
	// 1. Sync store users to Keycloak
	// usersFolderId := qdata.PathResolver

	// keycloakUsersByName, err := me.admin.GetUsers(ctx)
	// if err != nil {
	// 	return fmt.Errorf("failed to get Keycloak users: %w", err)
	// }

	// iterator := me.store.PrepareQuery("SELECT Name, SourceOfTruth, Parent FROM User WHERE SourceOfTruth = 'Keycloak'")
	// for _, user := range storeUsers {
	// 	storeUsersByName[user.GetField("Name").GetString()] = user

	// 	if usersFolderId == "" {
	// 		usersFolderId = user.GetField("Parent").GetEntityReference()
	// 	}

	// 	// Only sync users where store is source of truth
	// 	if user.GetField("SourceOfTruth").GetCompleteChoice(ctx).Option() != "QOS" {
	// 		continue
	// 	}

	// 	// Create or update user in Keycloak
	// 	username := user.GetField("Name").GetString()
	// 	if _, ok := keycloakUsersByName[username]; !ok {
	// 		qlog.Info("Creating QOS user '%s' in Keycloak", username)
	// 		if err := me.admin.CreateUser(ctx, username, username); err != nil {
	// 			qlog.Error("Failed to sync user %s to Keycloak: %v", username, err)
	// 		}
	// 	}
	// }

	// if usersFolderId == "" {
	// 	return fmt.Errorf("users folder not found")
	// }

	// 2. Sync Keycloak users to store
	// for _, kcUser := range keycloakUsersByName {
	// 	if user, ok := storeUsersByName[kcUser.GetUsername()]; !ok {
	// 		qlog.Info("Creating QOS user '%s' from Keycloak", kcUser.GetUsername())
	// 		userId := me.store.CreateEntity(ctx, "User", usersFolderId, kcUser.GetUsername())
	// 		user = qbinding.NewEntity(ctx, me.store, userId)
	// 		user.DoMulti(ctx, func(userBinding qdata.EntityBinding) {
	// 			userBinding.GetField("SourceOfTruth").WriteChoice(ctx, "Keycloak")
	// 			userBinding.GetField("KeycloakId").WriteString(ctx, kcUser.GetID())
	// 			userBinding.GetField("Email").WriteString(ctx, kcUser.GetEmail())
	// 			userBinding.GetField("FirstName").WriteString(ctx, kcUser.GetFirstName())
	// 			userBinding.GetField("LastName").WriteString(ctx, kcUser.GetLastName())
	// 			userBinding.GetField("IsEmailVerified").WriteBool(ctx, kcUser.IsEmailVerified())
	// 			userBinding.GetField("IsEnabled").WriteBool(ctx, kcUser.IsEnabled())
	// 			userBinding.GetField("JSON").WriteString(ctx, kcUser.JSON())
	// 		})
	// 	} else {
	// 		qlog.Info("Updating QOS user '%s' from Keycloak", kcUser.GetUsername())
	// 		user.DoMulti(ctx, func(userBinding qdata.EntityBinding) {
	// 			userBinding.GetField("KeycloakId").WriteString(ctx, kcUser.GetID())
	// 			userBinding.GetField("Email").WriteString(ctx, kcUser.GetEmail())
	// 			userBinding.GetField("FirstName").WriteString(ctx, kcUser.GetFirstName())
	// 			userBinding.GetField("LastName").WriteString(ctx, kcUser.GetLastName())
	// 			userBinding.GetField("IsEmailVerified").WriteBool(ctx, kcUser.IsEmailVerified())
	// 			userBinding.GetField("IsEnabled").WriteBool(ctx, kcUser.IsEnabled())
	// 			userBinding.GetField("JSON").WriteString(ctx, kcUser.JSON())
	// 		})
	// 	}
	// }

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
