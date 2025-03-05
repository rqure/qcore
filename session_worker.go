package main

import (
	"context"
	"time"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/auth"
	"github.com/rqure/qlib/pkg/data"
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
			log.Info("Full sync complete")
		case <-me.eventPollTimer.C:
			log.Info("Processing new session events...")
			err := me.eventEmitter.ProcessNextBatch(ctx, session)
			if err != nil {
				log.Error("Failed to process new session events: %v", err)
				return
			}
			log.Info("Processing new session events complete")
		}
	default:
		log.Panic("Unknown state")
	}
}

func (me *sessionWorker) OnStoreConnected(ctx context.Context) {
	me.isStoreConnected = true

	sessionControllers := query.New(me.store).
		Select().
		From("SessionController").
		Execute(ctx)

	for _, sessionController := range sessionControllers {
		lastEventTime := sessionController.GetField("LastEventTime").ReadTimestamp(ctx)
		me.eventEmitter.SetLastEventTime(lastEventTime)
	}
}

func (me *sessionWorker) OnStoreDisconnected(context.Context) {
	me.isStoreConnected = false
}

func (me *sessionWorker) onEvent(ctx context.Context, event auth.Event) {
	sessionControllers := query.New(me.store).
		Select().
		From("SessionController").
		Execute(ctx)

	for _, sessionController := range sessionControllers {
		sessionController.GetField("LastEventTime").WriteTimestamp(ctx, time.Now())
	}
}
