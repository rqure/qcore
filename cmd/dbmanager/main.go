package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qbinding"
	"github.com/rqure/qlib/pkg/qdata/qentity"
	"github.com/rqure/qlib/pkg/qdata/qfield"
	"github.com/rqure/qlib/pkg/qdata/qquery"
	"github.com/rqure/qlib/pkg/qdata/qstore"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

const (
	defaultPostgresAddr = "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable"
	defaultTimeout      = 30 * time.Second
)

var (
	postgresAddr string
	create       bool
	drop         bool
	keycloakDB   bool
	qstoreDB     bool
	timeout      int
	confirm      bool
	initialize   bool
)

func init() {
	flag.StringVar(&postgresAddr, "postgres", getEnvOrDefault("Q_POSTGRES_ADDR", defaultPostgresAddr), "PostgreSQL connection string")
	flag.BoolVar(&create, "create", false, "Create databases")
	flag.BoolVar(&drop, "drop", false, "Drop databases")
	flag.BoolVar(&initialize, "initialize", false, "Initialize database schemas")
	flag.BoolVar(&qstoreDB, "qstore", false, "Manage qstore database")
	flag.BoolVar(&keycloakDB, "keycloak", false, "Manage keycloak database")
	flag.IntVar(&timeout, "timeout", 30, "Connection timeout in seconds")
	flag.BoolVar(&confirm, "confirm", false, "Confirm destructive operations without prompt")
	flag.Parse()
}

func getEnvOrDefault(env, defaultVal string) string {
	if val := os.Getenv(env); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	if !qstoreDB && !keycloakDB {
		qlog.Info("No database selected. Use -qstore or -keycloak flags.")
		return
	}

	if !create && !drop && !initialize {
		qlog.Info("No operation selected. Use -create, -drop, or -initialize flags.")
		return
	}

	if drop && !confirm {
		qlog.Warn("WARNING: Drop operation requires confirmation. Add -confirm flag to proceed.")
		return
	}

	// Connect to default PostgreSQL database for database operations
	pool, err := pgxpool.New(ctx, postgresAddr)
	if err != nil {
		qlog.Panic("Failed to connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		qlog.Panic("Failed to ping PostgreSQL: %v", err)
	}

	qlog.Info("Connected to PostgreSQL server")

	if qstoreDB {
		if create {
			qlog.Info("Creating qstore database...")
			if err := createQStoreDatabase(ctx, pool); err != nil {
				qlog.Error("Failed to create qstore database: %v", err)
			} else {
				qlog.Info("qstore database created successfully")
			}
		}

		if initialize {
			qlog.Info("Initializing qstore database schema...")
			if err := initializeQStoreSchema(ctx); err != nil {
				qlog.Error("Failed to initialize qstore schema: %v", err)
			} else {
				qlog.Info("qstore schema initialized successfully")
			}
		}

		if drop {
			qlog.Info("Dropping qstore database...")
			if err := dropQStoreDatabase(ctx, pool); err != nil {
				qlog.Error("Failed to drop qstore database: %v", err)
			} else {
				qlog.Info("qstore database dropped successfully")
			}
		}
	}

	if keycloakDB {
		if create {
			qlog.Info("Creating keycloak database...")
			if err := createKeycloakDatabase(ctx, pool); err != nil {
				qlog.Error("Failed to create keycloak database: %v", err)
			} else {
				qlog.Info("keycloak database created successfully")
			}
		}

		if drop {
			qlog.Info("Dropping keycloak database...")
			if err := dropKeycloakDatabase(ctx, pool); err != nil {
				qlog.Error("Failed to drop keycloak database: %v", err)
			} else {
				qlog.Info("keycloak database dropped successfully")
			}
		}
	}

	qlog.Info("Database operations completed")
}

func createQStoreDatabase(ctx context.Context, pool *pgxpool.Pool) error {
	// Check if qstore database exists
	var dbExists bool
	err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'qstore')").Scan(&dbExists)
	if err != nil {
		return fmt.Errorf("failed to check if qstore database exists: %w", err)
	}

	if dbExists {
		qlog.Info("qstore database already exists")
	} else {
		// Create qstore database
		_, err = pool.Exec(ctx, "CREATE DATABASE qstore")
		if err != nil {
			return fmt.Errorf("failed to create qstore database: %w", err)
		}
		qlog.Info("qstore database created")
	}

	// Create qcore user if it doesn't exist
	_, err = pool.Exec(ctx, `
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'qcore') THEN
				CREATE USER qcore WITH PASSWORD 'qcore';
			ELSE
				ALTER USER qcore WITH PASSWORD 'qcore';
			END IF;
		END
		$$;
	`)
	if err != nil {
		return fmt.Errorf("failed to create qcore user: %w", err)
	}
	qlog.Info("qcore user created/updated")

	// Grant privileges to qcore user on the qstore database
	_, err = pool.Exec(ctx, "GRANT ALL PRIVILEGES ON DATABASE qstore TO qcore")
	if err != nil {
		return fmt.Errorf("failed to grant privileges to qcore user: %w", err)
	}
	qlog.Info("privileges granted to qcore user")

	// Connect to the qstore database specifically to grant schema permissions
	qstoreConnString := strings.Replace(postgresAddr, "/postgres?", "/qstore?", 1)
	qstorePool, err := pgxpool.New(ctx, qstoreConnString)
	if err != nil {
		return fmt.Errorf("failed to connect to qstore database: %w", err)
	}
	defer qstorePool.Close()

	// Grant schema permissions to qcore user
	_, err = qstorePool.Exec(ctx, "GRANT ALL ON SCHEMA public TO qcore")
	if err != nil {
		return fmt.Errorf("failed to grant schema permissions to qcore user: %w", err)
	}
	qlog.Info("schema permissions granted to qcore user")

	// Set qcore user as owner of public schema in qstore database
	_, err = qstorePool.Exec(ctx, "ALTER SCHEMA public OWNER TO qcore")
	if err != nil {
		return fmt.Errorf("failed to set schema owner: %w", err)
	}
	qlog.Info("schema ownership set for qcore user")

	return nil
}

func dropQStoreDatabase(ctx context.Context, pool *pgxpool.Pool) error {
	// Check if qstore database exists
	var dbExists bool
	err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'qstore')").Scan(&dbExists)
	if err != nil {
		return fmt.Errorf("failed to check if qstore database exists: %w", err)
	}

	if !dbExists {
		qlog.Info("qstore database doesn't exist, nothing to drop")
		return nil
	}

	// Terminate active connections to the database
	_, err = pool.Exec(ctx, `
		SELECT pg_terminate_backend(pg_stat_activity.pid)
		FROM pg_stat_activity
		WHERE pg_stat_activity.datname = 'qstore'
		AND pid <> pg_backend_pid()
	`)
	if err != nil {
		qlog.Warn("Failed to terminate connections to qstore database: %v", err)
		// Continue anyway
	}

	// Drop qstore database
	_, err = pool.Exec(ctx, "DROP DATABASE qstore")
	if err != nil {
		return fmt.Errorf("failed to drop qstore database: %w", err)
	}

	qlog.Info("qstore database dropped")
	return nil
}

func createKeycloakDatabase(ctx context.Context, pool *pgxpool.Pool) error {
	// Check if keycloak database exists
	var dbExists bool
	err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'keycloak')").Scan(&dbExists)
	if err != nil {
		return fmt.Errorf("failed to check if keycloak database exists: %w", err)
	}

	if dbExists {
		qlog.Info("keycloak database already exists")
	} else {
		// Create keycloak database
		_, err = pool.Exec(ctx, "CREATE DATABASE keycloak")
		if err != nil {
			return fmt.Errorf("failed to create keycloak database: %w", err)
		}
		qlog.Info("keycloak database created")
	}

	// Create keycloak user if it doesn't exist
	_, err = pool.Exec(ctx, `
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'keycloak') THEN
				CREATE USER keycloak WITH PASSWORD 'keycloak';
			ELSE
				ALTER USER keycloak WITH PASSWORD 'keycloak';
			END IF;
		END
		$$;
	`)
	if err != nil {
		return fmt.Errorf("failed to create keycloak user: %w", err)
	}
	qlog.Info("keycloak user created/updated")

	// Grant privileges to keycloak user on the keycloak database
	_, err = pool.Exec(ctx, "GRANT ALL PRIVILEGES ON DATABASE keycloak TO keycloak")
	if err != nil {
		return fmt.Errorf("failed to grant privileges to keycloak user: %w", err)
	}
	qlog.Info("privileges granted to keycloak user on database")

	// Connect to the keycloak database specifically to grant schema permissions
	keycloakConnString := strings.Replace(postgresAddr, "/postgres?", "/keycloak?", 1)
	keycloakPool, err := pgxpool.New(ctx, keycloakConnString)
	if err != nil {
		return fmt.Errorf("failed to connect to keycloak database: %w", err)
	}
	defer keycloakPool.Close()

	// Grant schema permissions to keycloak user
	_, err = keycloakPool.Exec(ctx, "GRANT ALL ON SCHEMA public TO keycloak")
	if err != nil {
		return fmt.Errorf("failed to grant schema permissions to keycloak user: %w", err)
	}
	qlog.Info("schema permissions granted to keycloak user")

	// Set keycloak user as owner of public schema in keycloak database
	_, err = keycloakPool.Exec(ctx, "ALTER SCHEMA public OWNER TO keycloak")
	if err != nil {
		return fmt.Errorf("failed to set schema owner: %w", err)
	}
	qlog.Info("schema ownership set for keycloak user")

	return nil
}

func dropKeycloakDatabase(ctx context.Context, pool *pgxpool.Pool) error {
	// Check if keycloak database exists
	var dbExists bool
	err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'keycloak')").Scan(&dbExists)
	if err != nil {
		return fmt.Errorf("failed to check if keycloak database exists: %w", err)
	}

	if !dbExists {
		qlog.Info("keycloak database doesn't exist, nothing to drop")
		return nil
	}

	// Terminate active connections to the database
	_, err = pool.Exec(ctx, `
		SELECT pg_terminate_backend(pg_stat_activity.pid)
		FROM pg_stat_activity
		WHERE pg_stat_activity.datname = 'keycloak'
		AND pid <> pg_backend_pid()
	`)
	if err != nil {
		qlog.Warn("Failed to terminate connections to keycloak database: %v", err)
		// Continue anyway
	}

	// Drop keycloak database
	_, err = pool.Exec(ctx, "DROP DATABASE keycloak")
	if err != nil {
		return fmt.Errorf("failed to drop keycloak database: %w", err)
	}

	qlog.Info("keycloak database dropped")
	return nil
}

func initializeQStoreSchema(ctx context.Context) error {
	// Connect to qstore database using the store interface
	qstoreConnString := strings.Replace(postgresAddr, "/postgres?", "/qstore?", 1)
	// Use qcore credentials for connecting
	qstoreConnString = strings.Replace(qstoreConnString, "postgres:postgres", "qcore:qcore", 1)

	// Create a store instance to interact with the database
	s := qstore.New(
		qstore.PersistOverPostgres(qstoreConnString),
	)

	// Connect to the database
	s.Connect(ctx)
	defer s.Disconnect(ctx)

	// Wait for connection to establish
	startTime := time.Now()
	timeout := 30 * time.Second
	for !s.IsConnected(ctx) {
		if time.Since(startTime) > timeout {
			return fmt.Errorf("timeout waiting for database connection")
		}
		time.Sleep(500 * time.Millisecond)
	}

	qlog.Info("Connected to qstore database")

	// Initialize the database if required
	s.InitializeIfRequired(ctx)

	// Create entity schemas (copied from InitStoreWorker.OnStoreConnected)
	ensureEntitySchema(ctx, s, qentity.FromSchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name: "Root",
		Fields: []*qprotobufs.DatabaseFieldSchema{
			{Name: "SchemaUpdateTrigger", Type: qfield.Choice, ChoiceOptions: []string{"Trigger"}},
		},
	}))

	ensureEntitySchema(ctx, s, qentity.FromSchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name:   "Folder",
		Fields: []*qprotobufs.DatabaseFieldSchema{},
	}))

	ensureEntitySchema(ctx, s, qentity.FromSchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name:   "Permission",
		Fields: []*qprotobufs.DatabaseFieldSchema{},
	}))

	ensureEntitySchema(ctx, s, qentity.FromSchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name:   "AreaOfResponsibility",
		Fields: []*qprotobufs.DatabaseFieldSchema{},
	}))

	ensureEntitySchema(ctx, s, qentity.FromSchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name: "Role",
		Fields: []*qprotobufs.DatabaseFieldSchema{
			{Name: "Permissions", Type: qfield.EntityList},
			{Name: "AreasOfResponsibilities", Type: qfield.EntityList},
		},
	}))

	ensureEntitySchema(ctx, s, qentity.FromSchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name: "User",
		Fields: []*qprotobufs.DatabaseFieldSchema{
			{Name: "Roles", Type: qfield.EntityList},
			{Name: "SelectedRole", Type: qfield.EntityReference},
			{Name: "Permissions", Type: qfield.EntityList},
			{Name: "TotalPermissions", Type: qfield.EntityList},
			{Name: "AreasOfResponsibilities", Type: qfield.EntityList},
			{Name: "SelectedAORs", Type: qfield.EntityReference},
			{Name: "SourceOfTruth", Type: qfield.Choice, ChoiceOptions: []string{"QOS", "Keycloak"}},
			{Name: "KeycloakId", Type: qfield.String},
			{Name: "Email", Type: qfield.String},
			{Name: "FirstName", Type: qfield.String},
			{Name: "LastName", Type: qfield.String},
			{Name: "IsEmailVerified", Type: qfield.Bool},
			{Name: "IsEnabled", Type: qfield.Bool},
			{Name: "JSON", Type: qfield.String},
		},
	}))

	ensureEntitySchema(ctx, s, qentity.FromSchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name: "Client",
		Fields: []*qprotobufs.DatabaseFieldSchema{
			{Name: "Permissions", Type: qfield.EntityList},
		},
	}))

	ensureEntitySchema(ctx, s, qentity.FromSchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name: "SessionController",
		Fields: []*qprotobufs.DatabaseFieldSchema{
			{Name: "LastEventTime", Type: qfield.Timestamp},
			{Name: "Logout", Type: qfield.EntityReference},
		},
	}))

	// Create root entity
	ensureEntity(ctx, s, "Root", "Root")

	// Create the security models
	ensureEntity(ctx, s, "Folder", "Root", "Security Models")

	ensureEntity(ctx, s, "Folder", "Root", "Security Models", "Permissions")
	systemPermission := ensureEntity(ctx, s, "Permission", "Root", "Security Models", "Permissions", "System")
	ensureEntity(ctx, s, "Permission", "Root", "Security Models", "Permissions", "System", "Security")
	ensureEntity(ctx, s, "Permission", "Root", "Security Models", "Permissions", "System", "Configuration")
	ensureEntity(ctx, s, "Permission", "Root", "Security Models", "Permissions", "System", "Application")

	ensureEntity(ctx, s, "Folder", "Root", "Security Models", "Areas of Responsibility")
	systemAor := ensureEntity(ctx, s, "AreaOfResponsibility", "Root", "Security Models", "Areas of Responsibility", "System")
	ensureEntity(ctx, s, "AreaOfResponsibility", "Root", "Security Models", "Areas of Responsibility", "System", "Database")

	ensureEntity(ctx, s, "Folder", "Root", "Security Models", "Roles")
	adminRole := ensureEntity(ctx, s, "Role", "Root", "Security Models", "Roles", "Admin")

	ensureEntity(ctx, s, "Folder", "Root", "Security Models", "Users")
	adminUser := ensureEntity(ctx, s, "User", "Root", "Security Models", "Users", "qei")

	ensureEntity(ctx, s, "Folder", "Root", "Security Models", "Clients")
	coreClient := ensureEntity(ctx, s, "Client", "Root", "Security Models", "Clients", "qcore")

	adminRole.DoMulti(ctx, func(role qdata.EntityBinding) {
		role.GetField("Permissions").WriteEntityList(ctx, []string{systemPermission.GetId()})
		role.GetField("AreasOfResponsibilities").WriteEntityList(ctx, []string{systemAor.GetId()})
	})

	adminUser.DoMulti(ctx, func(user qdata.EntityBinding) {
		user.GetField("Roles").WriteEntityList(ctx, []string{adminRole.GetId()})
		user.GetField("SourceOfTruth").WriteChoice(ctx, "QOS")
	})

	coreClient.DoMulti(ctx, func(client qdata.EntityBinding) {
		client.GetField("Permissions").WriteEntityList(ctx, []string{systemPermission.GetId()})
	})

	qlog.Info("Database schema initialization complete")
	return nil
}

// Helper functions moved from init_store_worker
func ensureEntitySchema(ctx context.Context, s qdata.Store, schema qdata.EntitySchema) {
	actualSchema := s.GetEntitySchema(ctx, schema.GetType())
	if actualSchema != nil {
		// Otherwise adding any missing fields to the actual schema
		for _, field := range schema.GetFields() {
			actualSchema.SetField(field.GetFieldName(), field)
		}
	} else {
		actualSchema = schema
	}

	s.SetEntitySchema(ctx, actualSchema)
	qlog.Info("Ensured entity schema: %s", schema.GetType())
}

func ensureEntity(ctx context.Context, store qdata.Store, entityType string, path ...string) qdata.EntityBinding {
	// The first element should be the root entity
	if len(path) == 0 {
		return nil
	}

	roots := qquery.New(store).
		Select("Name").
		From("Root").
		Where("Name").Equals(path[0]).
		Execute(ctx)

	var currentNode qdata.EntityBinding
	if len(roots) == 0 {
		if entityType == "Root" {
			qlog.Info("Creating root entity '%s'", path[0])
			rootId := store.CreateEntity(ctx, "Root", "", path[0])
			currentNode = qbinding.NewEntity(ctx, store, rootId)
		} else {
			qlog.Error("Root entity not found")
			return nil
		}
	} else {
		currentNode = roots[0]

		if len(roots) > 1 && len(path) > 1 {
			qlog.Warn("Multiple root entities found: %v", roots)
		} else if len(path) == 1 {
			return qbinding.NewEntity(ctx, store, currentNode.GetId())
		}
	}

	// Create the last item in the path
	// Return early if the intermediate entities are not found
	lastIndex := len(path) - 2
	for i, name := range path[1:] {
		children := currentNode.GetField("Children").ReadEntityList(ctx).GetEntities()

		found := false
		for _, childId := range children {
			child := qbinding.NewEntity(ctx, store, childId)
			if child.GetField("Name").ReadString(ctx) == name {
				currentNode = child
				found = true
				break
			}
		}

		if !found && i == lastIndex {
			qlog.Info("Creating entity '%s' (%d) in path '%s'", name, i+1, strings.Join(path, "/"))
			entityId := store.CreateEntity(ctx, entityType, currentNode.GetId(), name)
			return qbinding.NewEntity(ctx, store, entityId)
		} else if !found {
			qlog.Error("Entity '%s' (%d) not found in path '%s'", name, i+1, strings.Join(path, "/"))
			return nil
		}
	}

	if currentNode == nil {
		qlog.Error("Current node is nil: %s", strings.Join(path, "/"))
		return nil
	}

	return qbinding.NewEntity(ctx, store, currentNode.GetId())
}
