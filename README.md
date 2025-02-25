# qcore

qcore is a core service component that provides a central data storage and notification system. It implements a distributed architecture supporting read/write operations and real-time notifications.

## Features

- Distributed read/write operations with configurable modes
- Real-time notifications system with lease-based registration
- PostgreSQL-based persistent storage
- NATS-based message broker integration
- Configurable through environment variables

## Configuration

The service can be configured using the following environment variables:

- `Q_MODES` - Comma-separated list of operational modes (`reader`, `writer`)
- `Q_POSTGRES_ADDR` - PostgreSQL connection string (default: `postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable`)
- `Q_NATS_ADDR` - NATS server address (default: `nats://nats:4222`)
- `Q_WEB_ADDR` - Web service address (default: `0.0.0.0:20000`)

## Components

### Mode Manager
Controls the operational mode of the service:
- Reader Mode: Handles read requests, multiple instances can run for scalability
- Writer Mode: Handles write requests, single instance for consistency

### Workers

1. **Read Worker**
   - Handles all read operations
   - Supports entity queries, schema lookups, and database status checks
   - Only active in reader mode

2. **Write Worker**
   - Manages write operations
   - Ensures data consistency
   - Only active in writer mode

3. **Notification Worker**
   - Manages real-time notifications
   - Supports notification registration with lease-based expiration
   - Provides context-aware notifications

### Store Integration

The service integrates with:
- PostgreSQL for persistent storage
- NATS for message broker capabilities

## Usage

The service can be run in different modes depending on your scaling needs:

```bash
# Run as both reader and writer
export Q_MODES=reader,writer

# Run as read-only instance
export Q_MODES=reader

# Run as write-only instance
export Q_MODES=writer
