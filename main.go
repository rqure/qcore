package main

import (
	"os"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/app/workers"
	"github.com/rqure/qlib/pkg/data/store"
	"github.com/rqure/qlib/pkg/data/store/nats"
)

func getPostgresAddress() string {
	addr := os.Getenv("Q_POSTGRES_ADDR")
	if addr == "" {
		addr = "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable"
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
	natsCore := nats.NewCore(nats.Config{Address: getNatsAddress()})
	notificationManager := NewNotificationManager(natsCore)

	s := store.New(
		store.PersistOverPostgres(getPostgresAddress()),
		func(store *store.Store) {
			natsCore.SetSessionProvider(store.SessionProvider)
			store.MultiConnector.AddConnector(nats.NewConnector(natsCore))
			store.ModifiableNotificationConsumer = nats.NewNotificationConsumer(natsCore)
			store.ModifiableNotificationPublisher = notificationManager
		},
	)

	storeWorker := workers.NewStore(s)
	modeManager := NewModeManager()

	initStoreWorker := NewInitStoreWorker(s, modeManager)
	readWorker := NewReadWorker(s, natsCore, modeManager)
	writeWorker := NewWriteWorker(s, natsCore, modeManager)
	notificationWorker := NewNotificationWorker(s, natsCore, modeManager, notificationManager)

	sessionWorker := NewSessionWorker(s)

	// Connect store signals
	storeWorker.Connected.Connect(initStoreWorker.OnStoreConnected)
	storeWorker.Connected.Connect(readWorker.OnStoreConnected)
	storeWorker.Connected.Connect(writeWorker.OnStoreConnected)
	storeWorker.Connected.Connect(notificationWorker.OnStoreConnected)
	storeWorker.Connected.Connect(sessionWorker.OnStoreConnected)

	storeWorker.Disconnected.Connect(initStoreWorker.OnStoreDisconnected)
	storeWorker.Disconnected.Connect(readWorker.OnStoreDisconnected)
	storeWorker.Disconnected.Connect(writeWorker.OnStoreDisconnected)
	storeWorker.Disconnected.Connect(notificationWorker.OnStoreDisconnected)
	storeWorker.Disconnected.Connect(sessionWorker.OnStoreDisconnected)

	a := app.NewApplication("core")
	a.AddWorker(sessionWorker)
	a.AddWorker(initStoreWorker)
	a.AddWorker(storeWorker)
	a.AddWorker(readWorker)
	a.AddWorker(writeWorker)
	a.AddWorker(notificationWorker)
	a.Execute()
}
