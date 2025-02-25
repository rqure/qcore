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

func getWebServiceAddress() string {
	addr := os.Getenv("Q_WEB_ADDR")
	if addr == "" {
		addr = "0.0.0.0:20000"
	}

	return addr
}

func main() {
	natsCore := nats.NewCore(nats.Config{Address: getNatsAddress()})

	s := store.New(
		store.PersistOverPostgres(getPostgresAddress()),
		func(store *store.Store) {
			store.ModifiableNotificationConsumer = nats.NewNotificationConsumer(natsCore)
			store.ModifiableNotificationPublisher = nats.NewNotificationPublisher(natsCore)
		},
	)

	storeWorker := workers.NewStore(s)
	modeManager := NewModeManager()

	readWorker := NewReadWorker(s, natsCore, modeManager)
	writeWorker := NewWriteWorker(s, natsCore, modeManager)
	notificationWorker := NewNotificationWorker(s, natsCore, modeManager)

	// Connect store signals
	storeWorker.Connected.Connect(readWorker.OnStoreConnected)
	storeWorker.Connected.Connect(writeWorker.OnStoreConnected)
	storeWorker.Connected.Connect(notificationWorker.OnStoreConnected)

	storeWorker.Disconnected.Connect(readWorker.OnStoreDisconnected)
	storeWorker.Disconnected.Connect(writeWorker.OnStoreDisconnected)
	storeWorker.Disconnected.Connect(notificationWorker.OnStoreDisconnected)

	a := app.NewApplication("store")
	a.AddWorker(storeWorker)
	a.AddWorker(readWorker)
	a.AddWorker(writeWorker)
	a.AddWorker(notificationWorker)
	a.Execute()
}
