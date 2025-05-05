package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qapp/qworkers"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qstore"
	"github.com/rqure/qlib/pkg/qlog"
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
	keycloak     bool // add flag for keycloak reinit
)

func init() {
	flag.StringVar(&postgresAddr, "postgres", getEnvOrDefault("Q_POSTGRES_ADDR", defaultPostgresAddr), "PostgreSQL connection string")
	flag.IntVar(&timeout, "timeout", 30, "Connection timeout in seconds")
	flag.StringVar(&logLevel, "log-level", "INFO", "Set application log level (TRACE, DEBUG, INFO, WARN, ERROR, PANIC)")
	flag.StringVar(&libLogLevel, "lib-log-level", "INFO", "Set library log level (TRACE, DEBUG, INFO, WARN, ERROR, PANIC)")
	flag.BoolVar(&keycloak, "keycloak", false, "Reinitialize keycloak database (drop/create/init)")
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

	if keycloak {
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
	} else {
		qlog.Info("Skipping keycloak database reinitialization (--keycloak not set)")
	}

	initializeQStoreSchema()
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

func initializeQStoreSchema() {
	// Create a store instance to interact with the database
	s := qstore.New()

	a := qapp.NewApplication("qinitdb")
	oneShotWorker := qworkers.NewOneShot(s)
	oneShotWorker.Connected().Connect(func(ctx context.Context) {
		// Initialize the database if required
		s.InitializeSchema(ctx)

		s.RestoreSnapshot(ctx, new(qdata.Snapshot).Init())

		err := qstore.Initialize(ctx, s)

		if err != nil {
			qlog.Warn("Failed to ensure entity schema: %v", err)
			return
		}

		qlog.Info("Database schema initialization complete")
	})
	a.AddWorker(oneShotWorker)
	a.Execute()
}
