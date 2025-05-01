package main

import (
	"context"

	"github.com/rqure/qlib/pkg/qauthentication"
	"github.com/rqure/qlib/pkg/qauthorization"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
)

// verifyAuthentication is a helper method that verifies the user's authentication
// and returns a context with authorization if successful, or nil if authentication fails
func verifyAuthentication(ctx context.Context, accessToken string, store qdata.StoreInteractor) (context.Context, bool) {
	clientProvider := qcontext.GetClientProvider[qauthentication.Client](ctx)
	client := clientProvider.Client(ctx)
	if client == nil {
		qlog.Warn("Client not found")
		return nil, false
	}

	accessorSession := client.AccessTokenToSession(ctx, accessToken)
	if !accessorSession.IsValid(ctx) {
		qlog.Warn("Invalid session")
		return nil, false
	}

	accessorName, err := accessorSession.GetOwnerName(ctx)
	if err != nil {
		qlog.Warn("Could not get owner name: %v", err)
		return nil, false
	}

	found := false
	var authCtx context.Context

	// Check if accessor is a User
	iter, err := store.PrepareQuery(`SELECT "$EntityId" FROM User WHERE Name = %q`, accessorName)
	if err != nil {
		qlog.Warn("Could not prepare query: %v", err)
		return nil, false
	}

	iter.ForEach(ctx, func(row qdata.QueryRow) bool {
		user := row.AsEntity()
		authCtx = context.WithValue(
			ctx,
			qcontext.KeyAuthorizer,
			qauthorization.NewAuthorizer(user.EntityId, store))
		found = true
		return false // Break after first user
	})
	iter.Close()

	// If not found, check if accessor is a Client
	if !found {
		iter, err = store.PrepareQuery(`SELECT "$EntityId" FROM Client WHERE Name = %q`, accessorName)
		if err != nil {
			qlog.Warn("Could not prepare query: %v", err)
			return nil, false
		}

		iter.ForEach(ctx, func(row qdata.QueryRow) bool {
			client := row.AsEntity()
			authCtx = context.WithValue(
				ctx,
				qcontext.KeyAuthorizer,
				qauthorization.NewAuthorizer(client.EntityId, store))
			found = true
			return false // Break after first client
		})
		iter.Close()
	}

	// Special case for qinitdb client
	if !found && accessorName == "qinitdb" {
		qlog.Info("InitDB client detected, skipping authorization")
		authCtx = ctx // No special authorizer needed for qinitdb
		found = true
	}

	if !found {
		qlog.Warn("No matching user or client found for accessor name: %q", accessorName)
		return nil, false
	}

	return authCtx, true
}
