package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
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
	timeout      int
	logLevel     string
	libLogLevel  string
)

func init() {
	flag.StringVar(&postgresAddr, "postgres", getEnvOrDefault("Q_POSTGRES_ADDR", defaultPostgresAddr), "PostgreSQL connection string")
	flag.IntVar(&timeout, "timeout", 30, "Connection timeout in seconds")
	flag.StringVar(&logLevel, "log-level", "INFO", "Set application log level (TRACE, DEBUG, INFO, WARN, ERROR, PANIC)")
	flag.StringVar(&libLogLevel, "lib-log-level", "INFO", "Set library log level (TRACE, DEBUG, INFO, WARN, ERROR, PANIC)")
	flag.Parse()
}

func getEnvOrDefault(env, defaultVal string) string {
	if val := os.Getenv(env); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	// Set log levels before any other operations
	setLogLevel(logLevel, libLogLevel)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

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

	// Drop both databases first
	qlog.Info("Dropping existing databases...")
	if err := dropKeycloakDatabase(ctx, pool); err != nil {
		qlog.Error("Failed to drop keycloak database: %v", err)
	}

	// Create and initialize both databases
	qlog.Info("Creating and initializing databases...")
	if err := createKeycloakDatabase(ctx, pool); err != nil {
		qlog.Error("Failed to create keycloak database: %v", err)
	}

	if err := initializeQStoreSchema(context.WithValue(ctx, qcontext.KeyAppName, "qinitdb")); err != nil {
		qlog.Error("Failed to initialize qstore schema: %v", err)
	}

	qlog.Info("Database reinitialization completed")
}

func setLogLevel(appLevel, libLevel string) {
	levelMap := map[string]qlog.Level{
		"TRACE": qlog.TRACE,
		"DEBUG": qlog.DEBUG,
		"INFO":  qlog.INFO,
		"WARN":  qlog.WARN,
		"ERROR": qlog.ERROR,
		"PANIC": qlog.PANIC,
	}

	if level, ok := levelMap[strings.ToUpper(appLevel)]; ok {
		qlog.SetLevel(level)
	} else {
		qlog.Warn("Invalid log level '%s', using INFO", appLevel)
		qlog.SetLevel(qlog.INFO)
	}

	if level, ok := levelMap[strings.ToUpper(libLevel)]; ok {
		qlog.SetLibLevel(level)
	} else {
		qlog.Warn("Invalid lib log level '%s', using INFO", libLevel)
		qlog.SetLibLevel(qlog.INFO)
	}
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
	// Create a store instance to interact with the database
	s := qstore.New(
		qstore.PersistOverRedis("redis:6379", "", 0, 10),
	)

	// Connect to the database
	s.Connect(ctx)
	defer s.Disconnect(ctx)

	// Wait for connection to establish
	startTime := time.Now()
	timeout := 30 * time.Second
	for !s.IsConnected() {
		s.CheckConnection(ctx)
		if time.Since(startTime) > timeout {
			return fmt.Errorf("timeout waiting for database connection")
		}
		time.Sleep(500 * time.Millisecond)
	}

	qlog.Info("Connected to qstore database")

	// Initialize the database if required
	s.InitializeSchema(ctx)

	s.RestoreSnapshot(ctx, new(qdata.Snapshot).Init())

	// Create entity schemas (copied from InitStoreWorker.OnStoreConnected)
	ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name: qdata.ETRoot.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{
			{Name: qdata.FTSchemaChanged.AsString(), Type: qdata.VTString.AsString()},          // written value is the entity type that had its schema changed
			{Name: qdata.FTEntityCreated.AsString(), Type: qdata.VTEntityReference.AsString()}, // written value is the entity id that was created
			{Name: qdata.FTEntityDeleted.AsString(), Type: qdata.VTEntityReference.AsString()}, // written value is the entity id that was deleted
		},
	}))

	ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name:   qdata.ETFolder.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{},
	}))

	ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name: qdata.ETPermission.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{
			{Name: qdata.FTPolicy.AsString(), Type: qdata.VTString.AsString()},
		},
	}))

	ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name:   qdata.ETAreaOfResponsibility.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{},
	}))

	ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name:   qdata.ETRole.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{},
	}))

	ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name: qdata.ETUser.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{
			{Name: qdata.FTRoles.AsString(), Type: qdata.VTEntityList.AsString()},
			{Name: qdata.FTAreasOfResponsibilities.AsString(), Type: qdata.VTEntityList.AsString()},
			{Name: qdata.FTSourceOfTruth.AsString(), Type: qdata.VTChoice.AsString(), ChoiceOptions: []string{"QOS", "Keycloak"}},
			{Name: qdata.FTKeycloakId.AsString(), Type: qdata.VTString.AsString()},
			{Name: qdata.FTEmail.AsString(), Type: qdata.VTString.AsString()},
			{Name: qdata.FTFirstName.AsString(), Type: qdata.VTString.AsString()},
			{Name: qdata.FTLastName.AsString(), Type: qdata.VTString.AsString()},
			{Name: qdata.FTIsEmailVerified.AsString(), Type: qdata.VTBool.AsString()},
			{Name: qdata.FTIsEnabled.AsString(), Type: qdata.VTBool.AsString()},
			{Name: qdata.FTJSON.AsString(), Type: qdata.VTString.AsString()},
		},
	}))

	ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name: qdata.ETClient.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{
			{Name: qdata.FTLogLevel.AsString(), Type: qdata.VTChoice.AsString(), ChoiceOptions: []string{"Trace", "Debug", "Info", "Warn", "Error", "Panic"}},
			{Name: qdata.FTQLibLogLevel.AsString(), Type: qdata.VTChoice.AsString(), ChoiceOptions: []string{"Trace", "Debug", "Info", "Warn", "Error", "Panic"}},
		},
	}))

	ensureEntitySchema(ctx, s, new(qdata.EntitySchema).FromEntitySchemaPb(&qprotobufs.DatabaseEntitySchema{
		Name: qdata.ETSessionController.AsString(),
		Fields: []*qprotobufs.DatabaseFieldSchema{
			{Name: qdata.FTLastEventTime.AsString(), Type: qdata.VTTimestamp.AsString()},
			{Name: qdata.FTLogout.AsString(), Type: qdata.VTEntityReference.AsString()},
		},
	}))

	// Create root entity
	ensureEntity(ctx, s, qdata.ETRoot, "Root")

	// Create the security models
	ensureEntity(ctx, s, qdata.ETFolder, "Root", "Security Models")

	ensureEntity(ctx, s, qdata.ETFolder, "Root", "Security Models", "Permissions")
	ensureEntity(ctx, s, "Permission", "Root", "Security Models", "Permissions", "System")

	ensureEntity(ctx, s, qdata.ETFolder, "Root", "Security Models", "Areas of Responsibility")
	ensureEntity(ctx, s, "AreaOfResponsibility", "Root", "Security Models", "Areas of Responsibility", "System")

	ensureEntity(ctx, s, qdata.ETFolder, "Root", "Security Models", "Roles")
	adminRole := ensureEntity(ctx, s, "Role", "Root", "Security Models", "Roles", "Admin")

	ensureEntity(ctx, s, qdata.ETFolder, "Root", "Security Models", "Users")
	adminUser := ensureEntity(ctx, s, "User", "Root", "Security Models", "Users", "qei")

	ensureEntity(ctx, s, qdata.ETFolder, "Root", "Security Models", "Clients")
	ensureEntity(ctx, s, "Client", "Root", "Security Models", "Clients", "qcore")

	adminUser.Field("Roles").Value.FromEntityList([]qdata.EntityId{adminRole.EntityId})
	adminUser.Field("SourceOfTruth").Value.FromChoice(0)
	s.Write(ctx,
		adminUser.Field("Roles").AsWriteRequest(),
		adminUser.Field("SourceOfTruth").AsWriteRequest())

	qlog.Info("Database schema initialization complete")
	return nil
}

// Helper functions moved from init_store_worker
func ensureEntitySchema(ctx context.Context, s *qdata.Store, schema *qdata.EntitySchema) {
	actualSchema, err := s.GetEntitySchema(ctx, schema.EntityType)
	if err == nil {
		for _, field := range schema.Fields {
			actualSchema.Fields[field.FieldType] = field
		}
	} else {
		actualSchema = schema
	}

	s.SetEntitySchema(ctx, actualSchema)
	qlog.Info("Ensured entity schema: %s", schema.EntityType)
}

func ensureEntity(ctx context.Context, store *qdata.Store, entityType qdata.EntityType, path ...string) *qdata.Entity {
	// The first element should be the root entity
	if len(path) == 0 {
		return nil
	}

	iter, err := store.PrepareQuery(`SELECT "$EntityId" FROM Root WHERE Name = %q`, path[0])
	if err != nil {
		qlog.Error("Failed to prepare query: %v", err)
		return nil
	}
	defer iter.Close()

	var currentNode *qdata.Entity
	if !iter.Next(ctx) {
		if entityType == qdata.ETRoot {
			qlog.Info("Creating %s entity '%s'", qdata.ETRoot, path[0])
			root, err := store.CreateEntity(ctx, qdata.ETRoot, "", path[0])
			if err != nil {
				qlog.Warn("Failed to create root entity: %v", err)
				return nil
			}
			return new(qdata.Entity).Init(root.EntityId)
		} else {
			qlog.Error("Root entity not found")
			return nil
		}
	} else {
		currentNode = iter.Get().AsEntity()
	}

	// Create the last item in the path
	// Return early if the intermediate entities are not found
	lastIndex := len(path) - 2
	for i, name := range path[1:] {
		store.Read(ctx, currentNode.Field("Children").AsReadRequest())
		children := currentNode.Field("Children").Value.GetEntityList()

		found := false
		for _, childId := range children {
			child := new(qdata.Entity).Init(childId)

			store.Read(ctx,
				child.Field("Name").AsReadRequest(),
				child.Field("Children").AsReadRequest(),
			)

			if child.Field("Name").Value.GetString() == name {
				currentNode = child
				found = true
				break
			}
		}

		if !found && i == lastIndex {
			qlog.Info("Creating entity '%s' (%d) in path '%s'", name, i+1, strings.Join(path, "/"))
			et, err := store.CreateEntity(ctx, entityType, currentNode.EntityId, name)
			if err != nil {
				qlog.Error("Failed to create entity '%s': %v", name, err)
				return nil
			}
			return new(qdata.Entity).Init(et.EntityId)
		} else if !found {
			qlog.Error("Entity '%s' (%d) not found in path '%s'", name, i+1, strings.Join(path, "/"))
			return nil
		}
	}

	if currentNode == nil {
		qlog.Error("Current node is nil: %s", strings.Join(path, "/"))
		return nil
	}

	return new(qdata.Entity).Init(currentNode.EntityId)
}
