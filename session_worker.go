package main

import (
	"context"
	"os"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/log"
)

func getEnvOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	return value
}

func generateRealmConfigFromEnv() *RealmConfig {
	return &RealmConfig{
		AdminUsername: getEnvOrDefault("Q_KEYCLOAK_ADMIN_USERNAME", "admin"),
		AdminPassword: getEnvOrDefault("Q_KEYCLOAK_ADMIN_PASSWORD", "admin"),
		BaseURL:       getEnvOrDefault("Q_KEYCLOAK_BASE_URL", "http://keycloak:8080/auth"),
		MasterRealm:   getEnvOrDefault("Q_KEYCLOAK_MASTER_REALM", "master"),
	}
}

type SessionWorker interface {
	app.Worker
	OnStoreConnected(context.Context)
	OnStoreDisconnected(context.Context)
}

type sessionWorker struct {
	handle             app.Handle
	keycloakInteractor KeycloakInteractor
	isStoreConnected   bool
}

func NewSessionWorker() SessionWorker {
	return &sessionWorker{}
}

func (w *sessionWorker) Init(ctx context.Context, handle app.Handle) {
	w.handle = handle
}

func (w *sessionWorker) Deinit(context.Context) {}

func (w *sessionWorker) DoWork(ctx context.Context) {
	if !w.isStoreConnected {
		return
	}

	if w.keycloakInteractor == nil {
		ki, err := NewKeycloakInteractor(ctx, generateRealmConfigFromEnv())
		if err != nil {
			log.Error("Could not create keycloak interactor: %v", err)
			return
		}

		w.keycloakInteractor = ki
	}
}

func (w *sessionWorker) OnStoreConnected(context.Context) {
	w.isStoreConnected = true
}

func (w *sessionWorker) OnStoreDisconnected(context.Context) {
	w.isStoreConnected = false
}
