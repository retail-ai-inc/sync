# Sync

[![Go Report Card](https://goreportcard.com/badge/github.com/retail-ai-inc/sync)](https://goreportcard.com/report/github.com/retail-ai-inc/sync)
[![Coverage Status](https://codecov.io/gh/retail-ai-inc/sync/graph/badge.svg)](https://codecov.io/gh/retail-ai-inc/sync)
[![GoDoc](https://godoc.org/github.com/retail-ai-inc/sync?status.svg)](https://godoc.org/github.com/retail-ai-inc/sync)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![DeepWiki](https://img.shields.io/badge/docs-DeepWiki-blue)](https://deepwiki.com/retail-ai-inc/sync)

Synchronize Production NOSQL and SQL data to Standalone instances for Data scientists or other purposes. A **Go-based** tool to synchronize MongoDB or SQL data from a **MongoDB replica set** or **sharded cluster** or production SQL instance to a **standalone instance**, supports initial and incremental synchronization, including **indexes** with change stream monitoring.

> [!NOTE]
> Sync now supports MongoDB, MySQL, PostgreSQL, MariaDB, and Redis. Next, `Sync` will support Elasticsearch.
> `Sync` is **PII** proof.

## What is the problem
Let's assume you have a mid to big-size SaaS platform or service with multiple tech teams and stakeholders. Different teams have different requirements for analyzing the production data independently. However, the tech team doesn't want to allow all these stakeholders direct access to the production databases due to security and stability issues.

## A simple one-way solution
Create standalone databases outside of your production database servers with the same name as production and sync the production data of the specific tables or collections to the standalone database. **Sync** will do this for you.

## Supported Databases

- MongoDB (Sharded clusters, Replica sets)
- MySQL
- MariaDB 
- PostgreSQL (PostgreSQL version 10+ with logical replication enabled)
- Redis (Standalone, Sentinel; does not support cluster mode)

## High Level Design Diagram

### MongoDB sync (usual)
![image](https://github.com/user-attachments/assets/f600c3ae-a6bf-4d64-9a7b-6715456a146b)

### MongoDB sync (shard or replica set)

![image](https://github.com/user-attachments/assets/82cd3811-44bf-4d44-8ac8-9f32aace7a83)

### MySQL or MariaDB

![image](https://github.com/user-attachments/assets/65b23a4c-56db-4833-89a1-0f802af878bd)


## Features

- **Initial Sync**:
  - MongoDB: Bulk synchronization of data from the MongoDB cluster or MongoDB replica set to the standalone MongoDB instance.
  - MySQL/MariaDB: Initial synchronization using batch inserts (default batch size: 100 rows) from the source to the target if the target table is empty.
  - PostgreSQL: Initial synchronization using batch inserts (default batch size: 100 rows) from the source to the target using logical replication slots and the pgoutput plugin.
  - Redis: Supports full data synchronization for standalone Redis and Sentinel setups using Redis Streams and Keyspace Notifications.
- **Change Stream & Incremental Updates**:
  - MongoDB: Watches for real-time changes (insert, update, replace, delete) in the cluster's collections and reflects them in the standalone instance.
  - MySQL/MariaDB: Uses binlog replication events to capture and apply incremental changes to the target.
  - PostgreSQL: Uses WAL (Write-Ahead Log) with the pgoutput plugin to capture and apply incremental changes to the target.
  - Redis: Uses Redis Streams and Keyspace Notifications to capture and sync incremental changes in real-time.
- **Batch Processing & Concurrency**:  
  Handles synchronization in batches for optimized performance and supports parallel synchronization for multiple collections/tables.
- **Restart Resilience**: 
  Stores MongoDB resume tokens, MySQL binlog positions, PostgreSQL replication positions, and Redis stream offsets in configurable state files, allowing the tool to resume synchronization from the last known position after a restart.
  - **Note for Redis**: Redis does not support resuming from the last state after a sync interruption. If `Sync` is interrupted or crashes, it will restart the synchronization process by executing the initial sync method to retrieve all keys and sync them to the target database. This is due to limitations in Redis Streams and Keyspace Notifications, which do not provide a built-in mechanism to persist and resume stream offsets across restarts. As a result, the tool cannot accurately determine the last synced state and must perform a full resync to ensure data consistency.
- **UI Interface**:  
  - The tool has added a **UI interface**, making it easier to operate and monitor the synchronization process. For detailed UI documentation and usage guide, please see [READMEUI.md](READMEUI.md).

## Prerequisites
- For MongoDB sources:
  - A source MongoDB cluster (replica set or sharded cluster) with MongoDB version >= 4.0.
  - A target standalone MongoDB instance with write permissions.
- For MySQL/MariaDB sources:
  - A MySQL or MariaDB instance with binlog enabled (ROW or MIXED format recommended) and a user with replication privileges.
  - A target MySQL or MariaDB instance with write permissions.
- For PostgreSQL sources:
  - A PostgreSQL instance with logical replication enabled and a replication slot created.
  - A target PostgreSQL instance with write permissions.
- For Redis sources:
  - Redis standalone or Sentinel setup with Redis version >= 5.0.
  - Redis Streams and Keyspace Notifications enabled.
  - A target Redis instance with write permissions.



## Installation(For development)

After running `./sync`, the application will output a browser address:
- URL: [http://localhost:8080](http://localhost:8080)
- Username: `admin`
- Password: `admin`


```
# 1. Clone the repository:
git clone https://github.com/retail-ai-inc/sync.git
cd sync

# 2. Install dependencies
go mod tidy

# 3. Run the application
go run cmd/sync/main.go

# 4. Build the binary
go build -o sync cmd/sync/main.go

# 5. Build the Docker image
docker build -t sync .
docker run -d -p 8080:8080 sync
```


## Real-Time Synchronization

- MongoDB: Uses Change Streams from replica sets or sharded clusters for incremental updates.
- MySQL/MariaDB: Uses binlog replication to apply incremental changes to the target.
- PostgreSQL: Uses WAL (Write-Ahead Log) with the pgoutput plugin to apply incremental changes to the target.
- Redis: Uses Redis Streams and Keyspace Notifications to sync changes in real-time.
  - **Note for Redis**: If `Sync` is interrupted, Redis will restart the synchronization process with an initial sync of all keys to the target. This ensures data consistency but may increase synchronization time after interruptions.

On the restart, the tool resumes from the stored state (resume token for MongoDB, binlog position for MySQL/MariaDB, replication slot for PostgreSQL).

## Availability  

- MongoDB: MongoDB Change Streams require a replica set or sharded cluster. See [Convert Standalone to Replica Set](https://www.mongodb.com/docs/manual/tutorial/convert-standalone-to-replica-set/).
- MySQL/MariaDB: MySQL/MariaDB binlog-based incremental sync requires ROW or MIXED binlog format for proper event capturing.
- PostgreSQL: PostgreSQL incremental sync requires logical replication enabled with a replication slot.
- Redis: Redis sync supports standalone and Sentinel setups but does not support Redis Cluster mode. Redis does not support resuming from the last synced state after a crash or interruption.

## Contributing

We encourage all contributions to this repository! Please fork the repository or open an issue, make changes, and submit a pull request.
Note: All interactions here should conform to the [Code of Conduct](https://github.com/retail-ai-inc/sync/blob/main/CODE_OF_CONDUCT.md).

## Give a Star! ⭐

If you like or are using this project, please give it a **star**. Thanks!
