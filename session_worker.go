package main

import (
	"context"
	"fmt"
	"time"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/auth"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/binding"
	"github.com/rqure/qlib/pkg/data/query"
	"github.com/rqure/qlib/pkg/log"
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
	app.Worker
	OnStoreConnected(context.Context)
	OnStoreDisconnected(context.Context)
}

type sessionWorker struct {
	handle app.Handle

	store            data.Store
	isStoreConnected bool

	state SessionWorkerState

	core         auth.Core
	admin        auth.Admin
	eventEmitter auth.EventEmitter

	initTimer      *time.Timer
	fullSyncTimer  *time.Timer
	eventPollTimer *time.Timer
}

func NewSessionWorker(store data.Store) SessionWorker {
	return &sessionWorker{
		store: store,
		state: SessionWorkerState_Init,
	}
}

func (me *sessionWorker) Init(ctx context.Context, handle app.Handle) {
	me.handle = handle
	me.core = auth.NewCore()
	me.admin = auth.NewAdmin(me.core)
	me.eventEmitter = auth.NewEventEmitter(me.core)
	me.eventEmitter.Signal().Connect(me.handleKeycloakEvent)

	me.initTimer = time.NewTimer(initSyncInterval)
	me.fullSyncTimer = time.NewTimer(fullSyncInterval)
	me.eventPollTimer = time.NewTimer(eventPollInterval)
}

func (me *sessionWorker) Deinit(context.Context) {
	me.initTimer.Stop()
	me.fullSyncTimer.Stop()
	me.eventPollTimer.Stop()
}

func (me *sessionWorker) DoWork(ctx context.Context) {
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
			log.Info("Ensuring setup of auth database...")
			err := me.admin.EnsureSetup(ctx)
			if err != nil {
				log.Error("Failed to ensure setup: %v", err)
				return
			}

			log.Info("Setup of auth database complete")
			me.state = SessionWorkerState_Sync
		default:
			return
		}
	case SessionWorkerState_Sync:
		if !me.isStoreConnected {
			return
		}

		select {
		case <-me.fullSyncTimer.C:
			log.Info("Performing full sync...")
			me.performFullSync(ctx)
			log.Info("Full sync complete")
		case <-me.eventPollTimer.C:
			log.Info("Processing new session events...")
			err := me.eventEmitter.ProcessNextBatch(ctx, session)
			if err != nil {
				log.Error("Failed to process all new session events: %v", err)
				return
			}
			log.Info("Processing new session events complete")
		default:
			return
		}
	default:
		log.Panic("Unknown state")
	}
}

func (me *sessionWorker) OnStoreConnected(ctx context.Context) {
	me.isStoreConnected = true

	sessionControllers := query.New(me.store).
		Select("LastEventTime").
		From("SessionController").
		Execute(ctx)

	for _, sessionController := range sessionControllers {
		lastEventTime := sessionController.GetField("LastEventTime").GetTimestamp()
		me.eventEmitter.SetLastEventTime(lastEventTime)
	}
}

func (me *sessionWorker) OnStoreDisconnected(context.Context) {
	me.isStoreConnected = false
}

func (me *sessionWorker) handleKeycloakEvent(ctx context.Context, event auth.Event) {
	sessionControllers := query.New(me.store).
		Select().
		From("SessionController").
		Execute(ctx)

	for _, sessionController := range sessionControllers {
		sessionController.GetField("LastEventTime").WriteTimestamp(ctx, time.Now())
	}
}

func (me *sessionWorker) performFullSync(ctx context.Context) error {
	// 1. Sync store users to Keycloak
	storeUsers := query.New(me.store).
		Select("Name", "SourceOfTruth", "Parent").
		From("User").
		Execute(ctx)

	usersFolderId := ""

	storeUsersByName := make(map[string]data.EntityBinding)
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
			log.Info("Creating QOS user '%s' in Keycloak", username)
			if err := me.admin.CreateUser(ctx, username, username); err != nil {
				log.Error("Failed to sync user %s to Keycloak: %v", username, err)
			}
		}
	}

	if usersFolderId == "" {
		return fmt.Errorf("users folder not found")
	}

	// 2. Sync Keycloak users to store
	for _, kcUser := range keycloakUsersByName {
		if user, ok := storeUsersByName[kcUser.GetUsername()]; !ok {
			log.Info("Creating QOS user '%s' from Keycloak", kcUser.GetUsername())
			userId := me.store.CreateEntity(ctx, "User", usersFolderId, kcUser.GetUsername())
			user = binding.NewEntity(ctx, me.store, userId)
			user.DoMulti(ctx, func(userBinding data.EntityBinding) {
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
			log.Info("Updating QOS user '%s' from Keycloak", kcUser.GetUsername())
			user.DoMulti(ctx, func(userBinding data.EntityBinding) {
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
