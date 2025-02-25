package main

import (
	"context"
	"fmt"

	"github.com/Nerzal/gocloak/v13"
)

const (
	realmName        = "qcore-realm"
	clientName       = "qcore-client"
	defaultAdminRole = "admin"
)

type KeycloakInteractor interface {
}

type keycloakInteractor struct {
	client      *gocloak.GoCloak
	adminToken  *gocloak.JWT
	realmConfig *RealmConfig
}

type RealmConfig struct {
	AdminUsername string
	AdminPassword string
	BaseURL       string
	MasterRealm   string
}

func NewKeycloakInteractor(ctx context.Context, config *RealmConfig) (KeycloakInteractor, error) {
	ki := &keycloakInteractor{
		client:      gocloak.NewClient(config.BaseURL),
		realmConfig: config,
	}

	if err := ki.authenticate(ctx); err != nil {
		return nil, fmt.Errorf("authentication failed: %v", err)
	}

	if err := ki.ensureRealmExists(ctx); err != nil {
		return nil, fmt.Errorf("realm setup failed: %v", err)
	}

	if err := ki.ensureClientExists(ctx); err != nil {
		return nil, fmt.Errorf("client setup failed: %v", err)
	}

	if err := ki.ensureRolesExist(ctx); err != nil {
		return nil, fmt.Errorf("roles setup failed: %v", err)
	}

	return ki, nil
}

func (ki *keycloakInteractor) authenticate(ctx context.Context) error {
	token, err := ki.client.LoginAdmin(ctx, ki.realmConfig.AdminUsername, ki.realmConfig.AdminPassword, ki.realmConfig.MasterRealm)
	if err != nil {
		return err
	}
	ki.adminToken = token
	return nil
}

func (ki *keycloakInteractor) ensureRealmExists(ctx context.Context) error {
	_, err := ki.client.GetRealm(ctx, ki.adminToken.AccessToken, realmName)
	if err != nil {
		// Create realm if it doesn't exist
		realm := &gocloak.RealmRepresentation{
			Realm:   gocloak.StringP(realmName),
			Enabled: gocloak.BoolP(true),
		}
		_, err = ki.client.CreateRealm(ctx, ki.adminToken.AccessToken, *realm)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ki *keycloakInteractor) ensureClientExists(ctx context.Context) error {
	clients, err := ki.client.GetClients(ctx, ki.adminToken.AccessToken, realmName, gocloak.GetClientsParams{
		ClientID: gocloak.StringP(clientName),
	})
	if err != nil || len(clients) == 0 {
		client := &gocloak.Client{
			ClientID:                  gocloak.StringP(clientName),
			Enabled:                   gocloak.BoolP(true),
			StandardFlowEnabled:       gocloak.BoolP(true),
			DirectAccessGrantsEnabled: gocloak.BoolP(true),
		}
		_, err = ki.client.CreateClient(ctx, ki.adminToken.AccessToken, realmName, *client)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ki *keycloakInteractor) ensureRolesExist(ctx context.Context) error {
	role, err := ki.client.GetRealmRole(ctx, ki.adminToken.AccessToken, realmName, defaultAdminRole)
	if err != nil || role == nil {
		role := &gocloak.Role{
			Name: gocloak.StringP(defaultAdminRole),
		}
		_, err = ki.client.CreateRealmRole(ctx, ki.adminToken.AccessToken, realmName, *role)
		if err != nil {
			return err
		}
	}
	return nil
}
