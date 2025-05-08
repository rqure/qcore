package main

import (
	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qapp/qworkers"
	"github.com/rqure/qlib/pkg/qdata/qstore"
)

func main() {
	notificationManager := NewNotificationManager()
	store := qstore.New2()

	initWorker := NewInitWorker(store)
	storeWorker := qworkers.NewStore(store)
	storeWorker.Connected().Connect(initWorker.OnConnected)

	readWorker := NewReadWorker(store)
	writeWorker := NewWriteWorker(store)
	notificationWorker := NewNotificationWorker(store, notificationManager)
	sessionWorker := NewSessionWorker(store)

	readinessWorker := qworkers.NewReadiness()
	readinessWorker.AddCriteria(qworkers.NewStoreConnectedCriteria(storeWorker, readinessWorker))
	readinessWorker.AddCriteria(NewSessionReadyCriteria(sessionWorker))

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
