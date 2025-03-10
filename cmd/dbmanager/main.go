package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rqure/qlib/pkg/log"
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
)

func init() {
	flag.StringVar(&postgresAddr, "postgres", getEnvOrDefault("Q_POSTGRES_ADDR", defaultPostgresAddr), "PostgreSQL connection string")
	flag.BoolVar(&create, "create", false, "Create databases")
	flag.BoolVar(&drop, "drop", false, "Drop databases")
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

	if !create && !drop {
		log.Info("No operation selected. Use -create or -drop flags.")
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
	log.Info("privileges granted to keycloak user")

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
