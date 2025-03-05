package main

import (
	"context"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/binding"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/query"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
)

type InitStoreWorker interface {
	app.Worker
	OnStoreConnected(context.Context)
	OnStoreDisconnected()
}

type initStoreWorker struct {
	store       data.Store
	modeManager ModeManager
	init        bool
}

func NewInitStoreWorker(store data.Store, modeManager ModeManager) InitStoreWorker {
	return &initStoreWorker{
		store:       store,
		modeManager: modeManager,
	}
}

func (w *initStoreWorker) Deinit(ctx context.Context) {}

func (w *initStoreWorker) Init(ctx context.Context, handle app.Handle) {}

func (w *initStoreWorker) DoWork(ctx context.Context) {}

func (w *initStoreWorker) OnStoreConnected(ctx context.Context) {
	if !w.modeManager.HasModes(ModeWrite) {
		return
	}

	if w.init {
		return
	}

	w.init = true

	// Create any missing entity schemas
	w.ensureEntitySchema(ctx, entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{
		Name: "Root",
		Fields: []*protobufs.DatabaseFieldSchema{
			{Name: "SchemaUpdateTrigger", Type: field.Choice, ChoiceOptions: []string{"Trigger"}},
		},
	}))

	w.ensureEntitySchema(ctx, entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{
		Name:   "Folder",
		Fields: []*protobufs.DatabaseFieldSchema{},
	}))

	w.ensureEntitySchema(ctx, entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{
		Name:   "Permission", // Controls what a user can do in the system
		Fields: []*protobufs.DatabaseFieldSchema{},
	}))

	w.ensureEntitySchema(ctx, entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{
		Name:   "AreaOfResponsibility", // Controls what a user can see in the UI
		Fields: []*protobufs.DatabaseFieldSchema{},
	}))

	w.ensureEntitySchema(ctx, entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{
		Name: "Role",
		Fields: []*protobufs.DatabaseFieldSchema{
			{Name: "Permissions", Type: field.EntityList},             // All permissions assigned to the role
			{Name: "AreasOfResponsibilities", Type: field.EntityList}, // All areas of responsibility assigned to the role
		},
	}))

	w.ensureEntitySchema(ctx, entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{
		Name: "User",
		Fields: []*protobufs.DatabaseFieldSchema{
			{Name: "Roles", Type: field.EntityList},             // All roles assigned to the user
			{Name: "SelectedRole", Type: field.EntityReference}, // Selected role, changed by user directly
			{Name: "Permissions", Type: field.EntityList},       // Selected permissions, changed by user directly or via role selection
			{Name: "TotalPermissions", Type: field.EntityList},  // All permissions from roles, calculated by the session_worker
			{Name: "AreasOfResponsibilities", Type: field.EntityList},
			{Name: "SelectedAORs", Type: field.EntityReference},
			{Name: "SourceOfTruth", Type: field.Choice, ChoiceOptions: []string{"QOS", "Keycloak"}}, // Where the user information is coming from
		},
	}))

	w.ensureEntitySchema(ctx, entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{
		Name: "Client",
		Fields: []*protobufs.DatabaseFieldSchema{
			{Name: "Permissions", Type: field.EntityList}, // All permissions assigned to the client
		},
	}))

	// Create any missing entities
	w.ensureEntity(ctx, "Root", "Root")

	// Create the security models
	w.ensureEntity(ctx, "Folder", "Root", "Security Models")

	w.ensureEntity(ctx, "Folder", "Root", "Security Models", "Permissions")
	systemPermission := w.ensureEntity(ctx, "Permission", "Root", "Security Models", "Permissions", "System")
	w.ensureEntity(ctx, "Permission", "Root", "Security Models", "Permissions", "System", "Security")
	w.ensureEntity(ctx, "Permission", "Root", "Security Models", "Permissions", "System", "Configuration")
	w.ensureEntity(ctx, "Permission", "Root", "Security Models", "Permissions", "System", "Application")

	w.ensureEntity(ctx, "Folder", "Root", "Security Models", "Areas of Responsibility")
	systemAor := w.ensureEntity(ctx, "AreaOfResponsibility", "Root", "Security Models", "Areas of Responsibility", "System")
	w.ensureEntity(ctx, "AreaOfResponsibility", "Root", "Security Models", "Areas of Responsibility", "System", "Database")

	w.ensureEntity(ctx, "Folder", "Root", "Security Models", "Roles")
	adminRole := w.ensureEntity(ctx, "Role", "Root", "Security Models", "Roles", "Admin")

	w.ensureEntity(ctx, "Folder", "Root", "Security Models", "Users")
	adminUser := w.ensureEntity(ctx, "User", "Root", "Security Models", "Users", "qei")

	w.ensureEntity(ctx, "Folder", "Root", "Security Models", "Clients")
	coreClient := w.ensureEntity(ctx, "Client", "Root", "Security Models", "Clients", "core")

	adminRole.DoMulti(ctx, func(role data.EntityBinding) {
		role.GetField("Permissions").WriteEntityList(ctx, []string{systemPermission.GetId()})
		role.GetField("AreasOfResponsibilities").WriteEntityList(ctx, []string{systemAor.GetId()})
	})

	adminUser.DoMulti(ctx, func(user data.EntityBinding) {
		user.GetField("Roles").WriteEntityList(ctx, []string{adminRole.GetId()})
		user.GetField("SourceOfTruth").WriteChoice(ctx, "QOS")
	})

	coreClient.DoMulti(ctx, func(client data.EntityBinding) {
		client.GetField("Permissions").WriteEntityList(ctx, []string{systemPermission.GetId()})
	})
}

func (w *initStoreWorker) OnStoreDisconnected() {}

func (w *initStoreWorker) ensureEntitySchema(ctx context.Context, schema data.EntitySchema) {
	actualSchema := w.store.GetEntitySchema(ctx, schema.GetType())
	if actualSchema != nil {
		// Otherwise adding any missing fields to the actual schema
		for _, field := range schema.GetFields() {
			actualSchema.SetField(field.GetFieldName(), field)
		}
	} else {
		actualSchema = schema
	}

	w.store.SetEntitySchema(ctx, actualSchema)
}

func (w *initStoreWorker) ensureEntity(ctx context.Context, entityType string, path ...string) data.EntityBinding {
	// The first element should be the root entity
	if len(path) == 0 {
		return nil
	}

	roots := query.New(w.store).
		Select("Name").
		From("Root").
		Where("Name").Equals(path[0]).
		Execute(ctx)

	var currentNode data.Entity
	if len(roots) == 0 {
		if entityType == "Root" {
			rootId := w.store.CreateEntity(ctx, "Root", "", path[0])
			currentNode = w.store.GetEntity(ctx, rootId)
		} else {
			log.Error("Root entity not found")
			return nil
		}
	} else {
		currentNode = roots[0]

		if len(path) > 1 {
			log.Warn("Multiple root entities found: %v", roots)
		} else {
			return binding.NewEntity(ctx, w.store, currentNode.GetId())
		}
	}

	// Create the last item in the path
	// Return early if the intermediate entities are not found
	for i, name := range path[1:] {
		entity := query.New(w.store).
			Select("Name").
			From(entityType).
			Where("Name").Equals(name).
			Where("Parent").Equals(currentNode.GetId()).
			Execute(ctx)

		lastIndex := len(path) - 2
		if len(entity) == 0 && i == lastIndex {
			entityId := w.store.CreateEntity(ctx, entityType, currentNode.GetId(), name)
			return binding.NewEntity(ctx, w.store, entityId)
		} else {
			if len(entity) == 0 {
				log.Error("Entity '%s' (%d) not found in path %v", name, i+1, path)
				return nil
			} else if len(entity) > 1 {
				log.Warn("Multiple entities with name '%s' (%d) found in path '%v': %v", name, i+1, path, entity)
			}

			currentNode = entity[0]
		}
	}

	return binding.NewEntity(ctx, w.store, currentNode.GetId())
}
