package main

import (
	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qapp/qworkers"
	"github.com/rqure/qlib/pkg/qdata/qstore"
	"github.com/rqure/qlib/pkg/qdata/qstore/qnats"
)

func main() {
	natsCore := qnats.NewCore(qnats.NatsConfig{Address: qstore.DefaultNatsAddress()})
	notificationManager := NewNotificationManager(natsCore)

	store := qstore.New2(natsCore)

	initWorker := NewInitWorker(store)
	storeWorker := qworkers.NewStore(store)
	storeWorker.Connected().Connect(initWorker.OnConnected)

	modeManager := NewModeManager()

	readWorker := NewReadWorker(store, natsCore, modeManager)
	writeWorker := NewWriteWorker(store, natsCore, modeManager)
	notificationWorker := NewNotificationWorker(store, natsCore, modeManager, notificationManager)
	sessionWorker := NewSessionWorker(store, modeManager)
	readinessWorker := qworkers.NewReadiness()
	readinessWorker.AddCriteria(qworkers.NewStoreConnectedCriteria(storeWorker, readinessWorker))
	readinessWorker.AddCriteria(NewSessionReadyCriteria(sessionWorker))
	readinessWorker.AddCriteria(qworkers.NewSchemaValidityCriteria(storeWorker, store))

	natsCore.BeforeConnected().Connect(notificationWorker.OnBeforeStoreConnected)

	readinessWorker.BecameReady().Connect(readWorker.OnReady)
	readinessWorker.BecameReady().Connect(writeWorker.OnReady)
	readinessWorker.BecameReady().Connect(sessionWorker.OnReady)

	readinessWorker.BecameNotReady().Connect(readWorker.OnNotReady)
	readinessWorker.BecameNotReady().Connect(writeWorker.OnNotReady)
	readinessWorker.BecameNotReady().Connect(sessionWorker.OnNotReady)

	a := qapp.NewApplication("qcore")
	a.AddWorker(sessionWorker)
	a.AddWorker(storeWorker)
	a.AddWorker(readinessWorker)
	a.AddWorker(readWorker)
	a.AddWorker(writeWorker)
	a.AddWorker(notificationWorker)
	a.Execute()
}
