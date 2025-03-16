package main

import (
	"context"
	"fmt"
	"time"

	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qauth"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qbinding"
	"github.com/rqure/qlib/pkg/qdata/qquery"
	"github.com/rqure/qlib/pkg/qlog"
)

const (
	initSyncInterval  = 5 * time.Second
	fullSyncInterval  = 1 * time.Minute
	eventPollInterval = 1 * time.Second
)

type SessionWorkerState string

const (
	SessionWorkerState_Init SessionWorkerState = "Init"
	SessionWorkerState_Sync SessionWorkerState = "Sync"
)

type SessionWorker interface {
	qapp.Worker
	OnReady()
	OnNotReady()
}

type sessionWorker struct {
	handle qapp.Handle

	store   qdata.Store
	isReady bool

	state SessionWorkerState

	core         qauth.Core
	admin        qauth.Admin
	eventEmitter qauth.EventEmitter

	initTimer      *time.Ticker
	fullSyncTimer  *time.Ticker
	eventPollTimer *time.Ticker
}

func NewSessionWorker(store qdata.Store) SessionWorker {
	return &sessionWorker{
		store: store,
		state: SessionWorkerState_Init,
	}
}

func (me *sessionWorker) Init(ctx context.Context) {
	me.handle = qapp.GetHandle(ctx)
	me.core = qauth.NewCore()
	me.admin = qauth.NewAdmin(me.core)
	me.eventEmitter = qauth.NewEventEmitter(me.core)
	me.eventEmitter.Signal().Connect(me.handleKeycloakEvent)

	me.initTimer = time.NewTicker(initSyncInterval)
	me.fullSyncTimer = time.NewTicker(fullSyncInterval)
	me.eventPollTimer = time.NewTicker(eventPollInterval)
}

func (me *sessionWorker) Deinit(context.Context) {
	me.initTimer.Stop()
	me.fullSyncTimer.Stop()
	me.eventPollTimer.Stop()
}

func (me *sessionWorker) DoWork(ctx context.Context) {
	if !me.isReady {
		return
	}

	session := me.admin.Session(ctx)
	if session.IsValid(ctx) {
		if session.PastHalfLife(ctx) {
			session.Refresh(ctx)
		}
	}

	switch me.state {
	case SessionWorkerState_Init:
		select {
		case <-me.initTimer.C:
			qlog.Info("Ensuring setup of auth database...")
			err := me.admin.EnsureSetup(ctx)
			if err != nil {
				qlog.Error("Failed to ensure setup: %v", err)
				return
			}

			qlog.Info("Setup of auth database complete")
			me.state = SessionWorkerState_Sync
		default:
			return
		}
	case SessionWorkerState_Sync:
		select {
		case <-me.fullSyncTimer.C:
			qlog.Trace("Performing full sync...")
			me.performFullSync(ctx)
			qlog.Trace("Full sync complete")
		case <-me.eventPollTimer.C:
			qlog.Trace("Processing new session events...")
			err := me.eventEmitter.ProcessNextBatch(ctx, session)
			if err != nil {
				qlog.Error("Failed to process all new session events: %v", err)
				return
			}
			qlog.Trace("Processing new session events complete")
		default:
			return
		}
	default:
		qlog.Panic("Unknown state")
	}
}

func (me *sessionWorker) OnReady() {
	me.handle.DoInMainThread(func(ctx context.Context) {
		me.isReady = true

		sessionControllers := qquery.New(me.store).
			Select("LastEventTime").
			From("SessionController").
			Execute(ctx)

		for _, sessionController := range sessionControllers {
			lastEventTime := sessionController.GetField("LastEventTime").GetTimestamp()
			me.eventEmitter.SetLastEventTime(lastEventTime)
		}
	})
}

func (me *sessionWorker) OnNotReady() {
	me.isReady = false
}

func (me *sessionWorker) handleKeycloakEvent(ctx context.Context, event qauth.Event) {
	sessionControllers := qquery.New(me.store).
		Select().
		From("SessionController").
		Execute(ctx)

	for _, sessionController := range sessionControllers {
		sessionController.GetField("LastEventTime").WriteTimestamp(ctx, time.Now())
	}
}

func (me *sessionWorker) performFullSync(ctx context.Context) error {
	// 1. Sync store users to Keycloak
	storeUsers := qquery.New(me.store).
		Select("Name", "SourceOfTruth", "Parent").
		From("User").
		Execute(ctx)

	usersFolderId := ""

	storeUsersByName := make(map[string]qdata.EntityBinding)
	keycloakUsersByName, err := me.admin.GetUsers(ctx)
	if err != nil {
		return fmt.Errorf("failed to get Keycloak users: %w", err)
	}

	for _, user := range storeUsers {
		storeUsersByName[user.GetField("Name").GetString()] = user

		if usersFolderId == "" {
			usersFolderId = user.GetField("Parent").GetEntityReference()
		}

		// Only sync users where store is source of truth
		if user.GetField("SourceOfTruth").GetCompleteChoice(ctx).Option() != "QOS" {
			continue
		}

		// Create or update user in Keycloak
		username := user.GetField("Name").GetString()
		if _, ok := keycloakUsersByName[username]; !ok {
			qlog.Info("Creating QOS user '%s' in Keycloak", username)
			if err := me.admin.CreateUser(ctx, username, username); err != nil {
				qlog.Error("Failed to sync user %s to Keycloak: %v", username, err)
			}
		}
	}

	if usersFolderId == "" {
		return fmt.Errorf("users folder not found")
	}

	// 2. Sync Keycloak users to store
	for _, kcUser := range keycloakUsersByName {
		if user, ok := storeUsersByName[kcUser.GetUsername()]; !ok {
			qlog.Info("Creating QOS user '%s' from Keycloak", kcUser.GetUsername())
			userId := me.store.CreateEntity(ctx, "User", usersFolderId, kcUser.GetUsername())
			user = qbinding.NewEntity(ctx, me.store, userId)
			user.DoMulti(ctx, func(userBinding qdata.EntityBinding) {
				userBinding.GetField("SourceOfTruth").WriteChoice(ctx, "Keycloak")
				userBinding.GetField("KeycloakId").WriteString(ctx, kcUser.GetID())
				userBinding.GetField("Email").WriteString(ctx, kcUser.GetEmail())
				userBinding.GetField("FirstName").WriteString(ctx, kcUser.GetFirstName())
				userBinding.GetField("LastName").WriteString(ctx, kcUser.GetLastName())
				userBinding.GetField("IsEmailVerified").WriteBool(ctx, kcUser.IsEmailVerified())
				userBinding.GetField("IsEnabled").WriteBool(ctx, kcUser.IsEnabled())
				userBinding.GetField("JSON").WriteString(ctx, kcUser.JSON())
			})
		} else {
			qlog.Info("Updating QOS user '%s' from Keycloak", kcUser.GetUsername())
			user.DoMulti(ctx, func(userBinding qdata.EntityBinding) {
				userBinding.GetField("KeycloakId").WriteString(ctx, kcUser.GetID())
				userBinding.GetField("Email").WriteString(ctx, kcUser.GetEmail())
				userBinding.GetField("FirstName").WriteString(ctx, kcUser.GetFirstName())
				userBinding.GetField("LastName").WriteString(ctx, kcUser.GetLastName())
				userBinding.GetField("IsEmailVerified").WriteBool(ctx, kcUser.IsEmailVerified())
				userBinding.GetField("IsEnabled").WriteBool(ctx, kcUser.IsEnabled())
				userBinding.GetField("JSON").WriteString(ctx, kcUser.JSON())
			})
		}
	}

	return nil
}
