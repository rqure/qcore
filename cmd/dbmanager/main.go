package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/binding"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/query"
	"github.com/rqure/qlib/pkg/data/store"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
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
		log.Info("No database selected. Use -qstore or -keycloak flags.")
		return
	}

	if !create && !drop && !initialize {
		log.Info("No operation selected. Use -create, -drop, or -initialize flags.")
		return
	}

	if drop && !confirm {
		log.Warn("WARNING: Drop operation requires confirmation. Add -confirm flag to proceed.")
		return
	}

	// Connect to default PostgreSQL database for database operations
	pool, err := pgxpool.New(ctx, postgresAddr)
	if err != nil {
		log.Panic("Failed to connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		log.Panic("Failed to ping PostgreSQL: %v", err)
	}

	log.Info("Connected to PostgreSQL server")

	if qstoreDB {
		if create {
			log.Info("Creating qstore database...")
			if err := createQStoreDatabase(ctx, pool); err != nil {
				log.Error("Failed to create qstore database: %v", err)
			} else {
				log.Info("qstore database created successfully")
			}
		}

		if initialize {
			log.Info("Initializing qstore database schema...")
			if err := initializeQStoreSchema(ctx); err != nil {
				log.Error("Failed to initialize qstore schema: %v", err)
			} else {
				log.Info("qstore schema initialized successfully")
			}
		}

		if drop {
			log.Info("Dropping qstore database...")
			if err := dropQStoreDatabase(ctx, pool); err != nil {
				log.Error("Failed to drop qstore database: %v", err)
			} else {
				log.Info("qstore database dropped successfully")
			}
		}
	}

	if keycloakDB {
		if create {
			log.Info("Creating keycloak database...")
			if err := createKeycloakDatabase(ctx, pool); err != nil {
				log.Error("Failed to create keycloak database: %v", err)
			} else {
				log.Info("keycloak database created successfully")
			}
		}

		if drop {
			log.Info("Dropping keycloak database...")
			if err := dropKeycloakDatabase(ctx, pool); err != nil {
				log.Error("Failed to drop keycloak database: %v", err)
			} else {
				log.Info("keycloak database dropped successfully")
			}
		}
	}

	log.Info("Database operations completed")
}

func createQStoreDatabase(ctx context.Context, pool *pgxpool.Pool) error {
	// Check if qstore database exists
	var dbExists bool
	err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'qstore')").Scan(&dbExists)
	if err != nil {
		return fmt.Errorf("failed to check if qstore database exists: %w", err)
	}

	if dbExists {
		log.Info("qstore database already exists")
	} else {
		// Create qstore database
		_, err = pool.Exec(ctx, "CREATE DATABASE qstore")
		if err != nil {
			return fmt.Errorf("failed to create qstore database: %w", err)
		}
		log.Info("qstore database created")
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
	log.Info("qcore user created/updated")

	// Grant privileges to qcore user on the qstore database
	_, err = pool.Exec(ctx, "GRANT ALL PRIVILEGES ON DATABASE qstore TO qcore")
	if err != nil {
		return fmt.Errorf("failed to grant privileges to qcore user: %w", err)
	}
	log.Info("privileges granted to qcore user")

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
	log.Info("schema permissions granted to qcore user")

	// Set qcore user as owner of public schema in qstore database
	_, err = qstorePool.Exec(ctx, "ALTER SCHEMA public OWNER TO qcore")
	if err != nil {
		return fmt.Errorf("failed to set schema owner: %w", err)
	}
	log.Info("schema ownership set for qcore user")

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
		log.Info("qstore database doesn't exist, nothing to drop")
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
		log.Warn("Failed to terminate connections to qstore database: %v", err)
		// Continue anyway
	}

	// Drop qstore database
	_, err = pool.Exec(ctx, "DROP DATABASE qstore")
	if err != nil {
		return fmt.Errorf("failed to drop qstore database: %w", err)
	}

	log.Info("qstore database dropped")
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
		log.Info("keycloak database already exists")
	} else {
		// Create keycloak database
		_, err = pool.Exec(ctx, "CREATE DATABASE keycloak")
		if err != nil {
			return fmt.Errorf("failed to create keycloak database: %w", err)
		}
		log.Info("keycloak database created")
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
	log.Info("keycloak user created/updated")

	// Grant privileges to keycloak user on the keycloak database
	_, err = pool.Exec(ctx, "GRANT ALL PRIVILEGES ON DATABASE keycloak TO keycloak")
	if err != nil {
		return fmt.Errorf("failed to grant privileges to keycloak user: %w", err)
	}
	log.Info("privileges granted to keycloak user on database")

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
	log.Info("schema permissions granted to keycloak user")

	// Set keycloak user as owner of public schema in keycloak database
	_, err = keycloakPool.Exec(ctx, "ALTER SCHEMA public OWNER TO keycloak")
	if err != nil {
		return fmt.Errorf("failed to set schema owner: %w", err)
	}
	log.Info("schema ownership set for keycloak user")

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
		log.Info("keycloak database doesn't exist, nothing to drop")
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
		log.Warn("Failed to terminate connections to keycloak database: %v", err)
		// Continue anyway
	}

	// Drop keycloak database
	_, err = pool.Exec(ctx, "DROP DATABASE keycloak")
	if err != nil {
		return fmt.Errorf("failed to drop keycloak database: %w", err)
	}

	log.Info("keycloak database dropped")
	return nil
}

func initializeQStoreSchema(ctx context.Context) error {
	// Connect to qstore database using the store interface
	qstoreConnString := strings.Replace(postgresAddr, "/postgres?", "/qstore?", 1)
	// Use qcore credentials for connecting
	qstoreConnString = strings.Replace(qstoreConnString, "postgres:postgres", "qcore:qcore", 1)

	// Create a store instance to interact with the database
	s := store.New(
		store.PersistOverPostgres(qstoreConnString),
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

	log.Info("Connected to qstore database")

	// Initialize the database if required
	s.InitializeIfRequired(ctx)

	// Create entity schemas (copied from InitStoreWorker.OnStoreConnected)
	ensureEntitySchema(ctx, s, entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{
		Name: "Root",
		Fields: []*protobufs.DatabaseFieldSchema{
			{Name: "SchemaUpdateTrigger", Type: field.Choice, ChoiceOptions: []string{"Trigger"}},
		},
	}))

	ensureEntitySchema(ctx, s, entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{
		Name:   "Folder",
		Fields: []*protobufs.DatabaseFieldSchema{},
	}))

	ensureEntitySchema(ctx, s, entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{
		Name:   "Permission",
		Fields: []*protobufs.DatabaseFieldSchema{},
	}))

	ensureEntitySchema(ctx, s, entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{
		Name:   "AreaOfResponsibility",
		Fields: []*protobufs.DatabaseFieldSchema{},
	}))

	ensureEntitySchema(ctx, s, entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{
		Name: "Role",
		Fields: []*protobufs.DatabaseFieldSchema{
			{Name: "Permissions", Type: field.EntityList},
			{Name: "AreasOfResponsibilities", Type: field.EntityList},
		},
	}))

	ensureEntitySchema(ctx, s, entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{
		Name: "User",
		Fields: []*protobufs.DatabaseFieldSchema{
			{Name: "Roles", Type: field.EntityList},
			{Name: "SelectedRole", Type: field.EntityReference},
			{Name: "Permissions", Type: field.EntityList},
			{Name: "TotalPermissions", Type: field.EntityList},
			{Name: "AreasOfResponsibilities", Type: field.EntityList},
			{Name: "SelectedAORs", Type: field.EntityReference},
			{Name: "SourceOfTruth", Type: field.Choice, ChoiceOptions: []string{"QOS", "Keycloak"}},
			{Name: "KeycloakId", Type: field.String},
			{Name: "Email", Type: field.String},
			{Name: "FirstName", Type: field.String},
			{Name: "LastName", Type: field.String},
			{Name: "IsEmailVerified", Type: field.Bool},
			{Name: "IsEnabled", Type: field.Bool},
			{Name: "JSON", Type: field.String},
		},
	}))

	ensureEntitySchema(ctx, s, entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{
		Name: "Client",
		Fields: []*protobufs.DatabaseFieldSchema{
			{Name: "Permissions", Type: field.EntityList},
		},
	}))

	ensureEntitySchema(ctx, s, entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{
		Name: "SessionController",
		Fields: []*protobufs.DatabaseFieldSchema{
			{Name: "LastEventTime", Type: field.Timestamp},
			{Name: "Logout", Type: field.EntityReference},
		},
	}))

	// Create root entity
	ensureEntity(ctx, s, "Root", "Root")

	log.Info("Database schema initialization complete")
	return nil
}

// Helper functions moved from init_store_worker
func ensureEntitySchema(ctx context.Context, s data.Store, schema data.EntitySchema) {
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
	log.Info("Ensured entity schema: %s", schema.GetType())
}

func ensureEntity(ctx context.Context, s data.Store, entityType string, path ...string) data.EntityBinding {
	// The first element should be the root entity
	if len(path) == 0 {
		return nil
	}

	roots := query.New(s).
		Select("Name").
		From("Root").
		Where("Name").Equals(path[0]).
		Execute(ctx)

	var currentNode data.Entity
	if len(roots) == 0 {
		if entityType == "Root" {
			log.Info("Creating root entity '%s'", path[0])
			rootId := s.CreateEntity(ctx, "Root", "", path[0])
			currentNode = s.GetEntity(ctx, rootId)
		} else {
			log.Error("Root entity not found")
			return nil
		}
	} else {
		currentNode = roots[0]

		if len(roots) > 1 && len(path) > 1 {
			log.Warn("Multiple root entities found: %v", roots)
		} else if len(path) == 1 {
			return binding.NewEntity(ctx, s, currentNode.GetId())
		}
	}

	// Create the last item in the path
	// Return early if the intermediate entities are not found
	for i, name := range path[1:] {
		entity := query.New(s).
			Select("Name").
			From(entityType).
			Where("Name").Equals(name).
			Where("Parent").Equals(currentNode.GetId()).
			Execute(ctx)

		lastIndex := len(path) - 2
		if len(entity) == 0 && i == lastIndex {
			log.Info("Creating entity '%s' (%d) in path %v", name, i+1, path)
			entityId := s.CreateEntity(ctx, entityType, currentNode.GetId(), name)
			return binding.NewEntity(ctx, s, entityId)
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

	if currentNode == nil {
		return nil
	}

	return binding.NewEntity(ctx, s, currentNode.GetId())
}
