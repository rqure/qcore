package main

import (
	"context"

	"github.com/rqure/qlib/pkg/app"
)

type SessionWorker interface {
	app.Worker
	OnStoreConnected(context.Context)
	OnStoreDisconnected(context.Context)
}

type sessionWorker struct {
	handle app.Handle
}

func NewSessionWorker() SessionWorker {
	return &sessionWorker{}
}

func (w *sessionWorker) Init(ctx context.Context, handle app.Handle) {
	w.handle = handle
}

func (w *sessionWorker) Deinit(context.Context) {}

func (w *sessionWorker) DoWork(context.Context) {}

func (w *sessionWorker) OnStoreConnected(context.Context) {}

func (w *sessionWorker) OnStoreDisconnected(context.Context) {}
