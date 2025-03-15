package main

import (
	"os"

	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qapp/qworkers"
	"github.com/rqure/qlib/pkg/qdata/qstore"
	"github.com/rqure/qlib/pkg/qdata/qstore/qnats"
)

func getPostgresAddress() string {
	addr := os.Getenv("Q_POSTGRES_ADDR")
	if addr == "" {
		// Use qcore user and qstore database directly in the default connection string
		addr = "postgres://qcore:qcore@postgres:5432/qstore?sslmode=disable"
	}
	return addr
}

func getNatsAddress() string {
	addr := os.Getenv("Q_NATS_ADDR")
	if addr == "" {
		addr = "nats://nats:4222"
	}

	return addr
}

func main() {
	natsCore := qnats.NewCore(qnats.Config{Address: getNatsAddress()})
	notificationManager := NewNotificationManager(natsCore)

	s := qstore.New(
		qstore.PersistOverPostgres(getPostgresAddress()),
		func(store *qstore.Store) {
			natsCore.SetAuthProvider(store.AuthProvider)
			store.MultiConnector.AddConnector(qnats.NewConnector(natsCore))
			store.ModifiableNotificationConsumer = qnats.NewNotificationConsumer(natsCore)
			store.ModifiableNotificationPublisher = notificationManager
		},
	)

	storeWorker := qworkers.NewStore(s)
	modeManager := NewModeManager()

	readWorker := NewReadWorker(s, natsCore, modeManager)
	writeWorker := NewWriteWorker(s, natsCore, modeManager)
	notificationWorker := NewNotificationWorker(s, natsCore, modeManager, notificationManager)
	sessionWorker := NewSessionWorker(s)

	natsCore.BeforeConnected().Connect(notificationWorker.OnBeforeStoreConnected)

	// Connect store signals
	storeWorker.Connected.Connect(readWorker.OnStoreConnected)
	storeWorker.Connected.Connect(writeWorker.OnStoreConnected)
	storeWorker.Connected.Connect(sessionWorker.OnStoreConnected)

	storeWorker.Disconnected.Connect(readWorker.OnStoreDisconnected)
	storeWorker.Disconnected.Connect(writeWorker.OnStoreDisconnected)
	storeWorker.Disconnected.Connect(notificationWorker.OnStoreDisconnected)
	storeWorker.Disconnected.Connect(sessionWorker.OnStoreDisconnected)

	a := qapp.NewApplication("core")
	a.AddWorker(sessionWorker)
	a.AddWorker(storeWorker)
	a.AddWorker(readWorker)
	a.AddWorker(writeWorker)
	a.AddWorker(notificationWorker)
	a.Execute()
}
