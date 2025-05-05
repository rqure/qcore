package main

import (
	"context"

	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qstore"
	"github.com/rqure/qlib/pkg/qlog"
)

type InitWorker interface {
	qapp.Worker
	OnConnected(context.Context)
}

type initWorker struct {
	store *qdata.Store
}

func NewInitWorker(store *qdata.Store) InitWorker {
	return &initWorker{
		store: store,
	}
}

func (w *initWorker) Init(ctx context.Context) {
}

func (w *initWorker) Deinit(ctx context.Context) {
}

func (w *initWorker) DoWork(ctx context.Context) {
}

func (w *initWorker) OnConnected(ctx context.Context) {
	err := qstore.Initialize(ctx, w.store)
	if err != nil {
		qlog.Panic("Failed to initialize store: %v", err)
	}

	qlog.Info("Store initialized successfully")
}
