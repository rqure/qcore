package main

import (
	"context"
	"time"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/auth"
	"github.com/rqure/qlib/pkg/log"
)

type SessionWorkerState string

const (
	SessionWorkerState_Init         SessionWorkerState = "Init"
	SessionWorkerState_PeriodicSync SessionWorkerState = "PeriodicSync"
)

type SessionWorker interface {
	app.Worker
	OnStoreConnected(context.Context)
	OnStoreDisconnected(context.Context)
}

type sessionWorker struct {
	handle           app.Handle
	isStoreConnected bool
	core             auth.Core
	admin            auth.Admin
	state            SessionWorkerState

	initTimer *time.Timer
}

func NewSessionWorker() SessionWorker {
	return &sessionWorker{
		state: SessionWorkerState_Init,
	}
}

func (w *sessionWorker) Init(ctx context.Context, handle app.Handle) {
	w.handle = handle
	w.core = auth.NewCore()
	w.admin = auth.NewAdmin(w.core)

	w.initTimer = time.NewTimer(5 * time.Second)
}

func (w *sessionWorker) Deinit(context.Context) {
	w.initTimer.Stop()
}

func (w *sessionWorker) DoWork(ctx context.Context) {
	switch w.state {
	case SessionWorkerState_Init:
		select {
		case <-w.initTimer.C:
			log.Info("Ensuring setup of auth database...")
			err := w.admin.EnsureSetup(ctx)
			if err != nil {
				log.Error("Failed to ensure setup: %v", err)
				return
			}

			log.Info("Setup of auth database complete")
			w.state = SessionWorkerState_PeriodicSync
		default:
			return
		}
	case SessionWorkerState_PeriodicSync:
		if !w.isStoreConnected {
			return
		}

	default:
		log.Panic("Unknown state")
	}
}

func (w *sessionWorker) OnStoreConnected(context.Context) {
	w.isStoreConnected = true
}

func (w *sessionWorker) OnStoreDisconnected(context.Context) {
	w.isStoreConnected = false
}
