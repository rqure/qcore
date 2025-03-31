package main

import (
	"os"
	"time"

	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qapp/qworkers"
	"github.com/rqure/qlib/pkg/qdata"
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
	natsCore := qnats.NewCore(qnats.NatsConfig{Address: getNatsAddress()})
	notificationManager := NewNotificationManager(natsCore)

	s := new(qdata.Store).Init(
		qstore.PersistOverPostgres(
			getPostgresAddress(),
			qstore.WithMemcachedCache("memcached:11211", 5*time.Minute)),
		func(store *qdata.Store) {
			if store.StoreConnector == nil {
				store.StoreConnector = qstore.NewMultiConnector()
			}

			if connector, ok := store.StoreConnector.(qstore.MultiConnector); ok {
				connector.AddConnector(qnats.NewConnector(natsCore))
			} else {
				store.StoreConnector = qnats.NewConnector(natsCore)
			}

			store.StoreNotifier = qnats.NewStoreNotifier(natsCore)
		},
	)

	storeWorker := qworkers.NewStore(s)
	modeManager := NewModeManager()

	readWorker := NewReadWorker(s, natsCore, modeManager)
	writeWorker := NewWriteWorker(s, natsCore, modeManager)
	notificationWorker := NewNotificationWorker(s, natsCore, modeManager, notificationManager)
	sessionWorker := NewSessionWorker(s, modeManager)
	readinessWorker := qworkers.NewReadiness()
	readinessWorker.AddCriteria(qworkers.NewStoreConnectedCriteria(storeWorker, readinessWorker))
	readinessWorker.AddCriteria(qworkers.NewSchemaValidityCriteria(storeWorker, s))
	readinessWorker.AddCriteria(NewSessionReadyCriteria(sessionWorker))

	natsCore.BeforeConnected().Connect(notificationWorker.OnBeforeStoreConnected)

	// Connect store signals
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
