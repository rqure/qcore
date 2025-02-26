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
	// Client session management
	CreateClientSession(ctx context.Context, clientID, clientSecret string) (*gocloak.JWT, error)
	ValidateClientSession(ctx context.Context, accessToken string) (*gocloak.IntroSpectTokenResult, error)

	// User session management
	CreateUserSession(ctx context.Context, username, password string) (*gocloak.JWT, error)
	ValidateUserSession(ctx context.Context, accessToken string) (*gocloak.IntroSpectTokenResult, error)
	RefreshUserSession(ctx context.Context, refreshToken string) (*gocloak.JWT, error)
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

func (ki *keycloakInteractor) CreateClientSession(ctx context.Context, clientID, clientSecret string) (*gocloak.JWT, error) {
	token, err := ki.client.GetToken(ctx, realmName, gocloak.TokenOptions{
		ClientID:     &clientID,
		ClientSecret: &clientSecret,
		GrantType:    gocloak.StringP("client_credentials"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client session: %v", err)
	}
	return token, nil
}

func (ki *keycloakInteractor) ValidateClientSession(ctx context.Context, accessToken string) (*gocloak.IntroSpectTokenResult, error) {
	result, err := ki.client.RetrospectToken(ctx, accessToken, clientName, "", realmName)
	if err != nil {
		return nil, fmt.Errorf("failed to validate client session: %v", err)
	}
	if !*result.Active {
		return nil, fmt.Errorf("token is not active")
	}
	return result, nil
}

func (ki *keycloakInteractor) CreateUserSession(ctx context.Context, username, password string) (*gocloak.JWT, error) {
	token, err := ki.client.Login(ctx,
		clientName,
		"",
		realmName,
		username,
		password,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create user session: %v", err)
	}
	return token, nil
}

func (ki *keycloakInteractor) ValidateUserSession(ctx context.Context, accessToken string) (*gocloak.IntroSpectTokenResult, error) {
	result, err := ki.client.RetrospectToken(ctx, accessToken, clientName, "", realmName)
	if err != nil {
		return nil, fmt.Errorf("failed to validate user session: %v", err)
	}
	if !*result.Active {
		return nil, fmt.Errorf("token is not active")
	}
	return result, nil
}

func (ki *keycloakInteractor) RefreshUserSession(ctx context.Context, refreshToken string) (*gocloak.JWT, error) {
	token, err := ki.client.RefreshToken(ctx, refreshToken, clientName, "", realmName)
	if err != nil {
		return nil, fmt.Errorf("failed to refresh user session: %v", err)
	}
	return token, nil
}
