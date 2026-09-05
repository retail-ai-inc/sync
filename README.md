# Sync

[![Go Report Card](https://goreportcard.com/badge/github.com/retail-ai-inc/sync)](https://goreportcard.com/report/github.com/retail-ai-inc/sync)
[![Coverage Status](https://codecov.io/gh/retail-ai-inc/sync/graph/badge.svg)](https://codecov.io/gh/retail-ai-inc/sync)
[![GoDoc](https://godoc.org/github.com/retail-ai-inc/sync?status.svg)](https://godoc.org/github.com/retail-ai-inc/sync)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/retail-ai-inc/sync)

**Sync** is a real-time, one-way, and [PII](https://en.wikipedia.org/wiki/Personal_data)-proof database backup tool written in **Golang** to synchronize NOSQL and SQL data to standalone instances, GCS, using a UI and workflow engine for Data scientists, auditing, and various other purposes. **Sync** can synchronize MongoDB or SQL data from a **MongoDB replica set** or **sharded cluster**, or production SQL instance to a **standalone instance**, supports initial and incremental synchronization, including **indexes** with change stream monitoring.

> [!NOTE]
> - Sync now supports MongoDB, MySQL, PostgreSQL, MariaDB, and Redis. Next, `Sync` will support Elasticsearch.
> - `Sync` is **PII** proof, meaning you can select specific records, keys, columns from your source databases, and sync them into target databases using one-way **encryption** or **masking** methods.

## What is the problem
- Let's assume you have multiple tech teams and stakeholders. Different teams have different requirements for analyzing the production data independently. However, the tech team doesn't want to allow all these stakeholders direct access to the production databases due to security and stability issues.
- Another use case is to support your hot storage and cold storage policies. For example, allowing your smartphone app users to access only the latest 1 year of data from the hot storage, and serve the rest of the data on a request basis from cold storage.

## A simple one-way solution
Create standalone databases outside of your production database servers with the same name as the production databases and synchronize the production data of specific tables or collections to the standalone database. **Sync** will do this for you.

## Supported Databases

- MongoDB (Sharded clusters, Replica sets)
- MySQL
- MariaDB 
- PostgreSQL (PostgreSQL version 10+ with logical replication enabled)
- Redis (Standalone, Sentinel; does not support cluster mode)

## High-Level Design Diagram

### MongoDB sync (usual)
![image](https://github.com/user-attachments/assets/f600c3ae-a6bf-4d64-9a7b-6715456a146b)

### MongoDB sync (shard or replica set)

![image](https://github.com/user-attachments/assets/82cd3811-44bf-4d44-8ac8-9f32aace7a83)

### MySQL or MariaDB

![image](https://github.com/user-attachments/assets/65b23a4c-56db-4833-89a1-0f802af878bd)


## Features

- **UI Interface**:  
  - The tool has added a [**UI interface**](READMEUI.md), making it easier to operate and monitor the synchronization process. For detailed UI documentation and usage guidance, please refer to [READMEUI.md](READMEUI.md).
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
  Handles synchronization in batches for optimized performance and supports parallel synchronization across multiple collections or tables.
- **Restart Resilience**: 
  Stores MongoDB resume tokens, MySQL binlog positions, PostgreSQL replication positions, and Redis stream offsets in configurable state files, allowing the tool to resume synchronization from the last known position after a restart.
  - **Note for Redis**: Redis does not support resuming from the last state after a sync interruption. If `Sync` is interrupted or crashes, it will restart the synchronization process by executing the initial sync method to retrieve all keys and sync them to the target database. This is due to limitations in Redis Streams and Keyspace Notifications, which do not provide a built-in mechanism for persisting and resuming stream offsets across restarts. As a result, the tool cannot accurately determine the last synced state and must perform a full resync to ensure data consistency.

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


## Quick Start
### 1.Start with docker (For End Users)

```bash
docker run -d -p 8080:8080 \
  -e SYNC_ADMIN_PASSWORD='choose-one' \
  -v sync-state:/mnt/state \
  zhangyongguang/sync:latest
```

**Access the Web UI**:
- URL: [http://localhost:8080](http://localhost:8080)
- Username: `admin`
- Password: whatever `SYNC_ADMIN_PASSWORD` was set to on first start

The control database is created at `/mnt/state/sync.db` on first start, so the
volume is what carries the tasks across a restart. The image no longer ships a
database, and there is no default password: see
[The first sign-in](#the-first-sign-in).

### 2.Development Setup (For Developers)

```
# 1. Clone the repository:
git clone https://github.com/retail-ai-inc/sync.git
cd sync

# 2. Install dependencies
go mod tidy

# 3. Run the application
go run ./cmd/sync

# 4. Build the Docker image
docker build -t sync .
docker run -d -p 8080:8080 -e SYNC_ADMIN_PASSWORD='choose-one' -v sync-state:/mnt/state sync
```

**Access the Web UI**:
- URL: [http://localhost:8080](http://localhost:8080)
- Username: `admin`
- Password: whatever `SYNC_ADMIN_PASSWORD` was set to on first start

## Real-Time Synchronization

- MongoDB: Uses Change Streams from replica sets or sharded clusters for incremental updates.
- MySQL/MariaDB: Uses binlog replication to apply incremental changes to the target.
- PostgreSQL: Uses WAL (Write-Ahead Log) with the pgoutput plugin to apply incremental changes to the target.
- Redis: Uses Redis Streams and Keyspace Notifications to sync changes in real-time.
  - **Note for Redis**: If `Sync` is interrupted, Redis will restart the synchronization process with an initial sync of all keys to the target. This ensures data consistency but may increase synchronization time after interruptions.

Upon restart, the tool resumes from the stored state (resume token for MongoDB, binlog position for MySQL/MariaDB, or replication slot for PostgreSQL).

## Availability  

- MongoDB: MongoDB Change Streams require a replica set or sharded cluster. See [Convert Standalone to Replica Set](https://www.mongodb.com/docs/manual/tutorial/convert-standalone-to-replica-set/).
- MySQL/MariaDB: MySQL/MariaDB binlog-based incremental sync requires ROW binlog format and `binlog_row_image=FULL`. With `MINIMAL` the binlog carries only the changed columns, which cannot be told apart from real NULLs, so a task refuses to start rather than write NULL over columns nobody touched.
- PostgreSQL: PostgreSQL incremental sync requires logical replication enabled with a replication slot.
- Redis: Redis sync supports standalone, Sentinel and Cluster setups. Keyspace notifications are not a durable log, so a periodic full reconciliation pass is what makes the copy converge; Redis has no offset to resume from after an interruption.

## Operating it

### Field encryption

A table mapping can mark a field `masked` or `encrypted`. Masking is local — the
value never leaves in readable form. Encryption needs a key, and there is no
default one: a task that marks a field `encrypted` with neither `SYNC_FIELD_KEY`
nor `SYNC_CONFIG_KEY` set refuses to start, naming the field.

That is a change. Until recently the key was a literal in this repository, so
anything encrypted under it could be read by anyone with the source — the
configuration said the field was protected and it was not. If a target already
holds values written that way:

1. set `SYNC_FIELD_KEY` to a key of your own;
2. run the initial copy again for the affected collections — the writes are
   upserts, so every document is rewritten under the new key;
3. anything not re-copied stays readable with the old published key, which is
   still in this repository's history.

### The first sign-in

The control database is created by the program, on first start, at
`SYNC_DB_PATH`. It comes up with no accounts in it, so set
`SYNC_ADMIN_PASSWORD` before the first start: an empty user table plus that
variable creates one administrator called `admin`, and the log says so.

The variable is read **only when there is no account at all**. Leaving it set in
a manifest afterwards does nothing — it cannot reset a password that has been
changed since, and it cannot put back an administrator that was removed on
purpose. Change the password after signing in, and the variable stops mattering.

Earlier versions shipped `sync.db` inside the repository with an administrator
row already in it, which meant one published password worked on every
deployment. That file is no longer tracked; a clone no longer carries anybody's
credentials, and neither does it carry their task configuration.

### Where the state lives

Two different kinds of state, with different durability requirements:

| State | Where it lives | Lost when |
| --- | --- | --- |
| Replication position (MongoDB resume token, MySQL GTID/binlog position, PostgreSQL LSN) and the replication direction lock | The **target** database, in `_sync_checkpoint` and `_sync_direction_lock` | Never, short of losing the target |
| Task definitions, users, sync history, backup jobs | One local **SQLite** file at `SYNC_DB_PATH` | The filesystem holding it goes away |
| MongoDB change buffer and dead letters | Local directories under the task's configured path (`./mongodb_buffer` by default) | The filesystem holding them goes away — see below |

Positions are on the target on purpose: a syncer that is rescheduled, or rebuilt
in another region, resumes from where the data actually is rather than from
whatever a local disk happened to keep.

### The control plane needs one replica and a persistent volume

`SYNC_DB_PATH` is a single SQLite file, and that has two consequences worth
planning around rather than discovering:

- **It must be on a persistent volume.** On Kubernetes without one, a
  rescheduled pod comes up with no tasks at all: replication stops until
  somebody re-enters the configuration. Replication positions survive, since
  they are on the target, but nothing knows to go and use them.
- **Only one replica may run.** SQLite takes one writer, and the pool is capped
  at one connection. Two replicas sharing a volume contend for the same file;
  two replicas on separate volumes each run every task twice, writing the same
  rows to the same target. Use `replicas: 1` with the `Recreate` strategy, and
  let the scheduler restart the pod rather than run a second one beside it.

The MongoDB buffer directory needs a persistent volume for a different reason:
it holds changes that have been read from the source but not yet applied to the
target. On `emptyDir`, a reschedule while the target is unreachable drops them.
`SYNC_MONGO_BUFFER_LIMIT_BYTES` caps the directory and applies backpressure to
the change stream when it fills, so a long target outage stops replication
loudly instead of filling the disk.

### Environment

| Variable | Effect |
| --- | --- |
| `SYNC_DB_PATH` | Path to the control-plane SQLite file. Defaults to `sync.db` beside the binary. The file and its schema are created on first start. |
| `SYNC_ADMIN_PASSWORD` | Password for the first administrator, created only when the user table is empty. Ignored once an account exists. |
| `SYNC_CONFIG_KEY` | 32-byte key, base64 or hex, that encrypts the database passwords stored in the task configuration. Without it they are stored in clear text and startup says so. |
| `SYNC_TOKEN_SECRET` | Signing secret for API tokens. Without it a generated one is used, so tokens do not survive a restart. |
| `SYNC_FIELD_KEY` | 32-byte key, base64 or hex, that encrypts the fields a task marks `encrypted`. `SYNC_CONFIG_KEY` is used when this is unset. A task that marks a field `encrypted` and has neither will not start. |
| `SYNC_PASSWORD_ITERATIONS` | Work factor for hashing the interface's own passwords. The default costs a noticeable fraction of a second per login; lower it only on slower hardware. |
| `SYNC_LAG_ALERT_SECONDS` | Replication lag, in seconds, past which a task is reported as alerting. |
| `SYNC_MONGO_BUFFER_LIMIT_BYTES` | Cap on the MongoDB change buffer directory. |
| `SYNC_MONGO_FLUSH_INTERVAL` | How long a partly filled batch of MongoDB changes waits before being applied, e.g. `200ms`. Default `500ms`. Lower is a tighter recovery point at the cost of more, smaller writes. |
| `SYNC_MONGO_NO_TRANSACTION` | `1` applies each MongoDB batch as a bare bulk write rather than inside a transaction. A batch interrupted part way is then applied in part while the position moves past it, so the target is quietly missing changes. It exists because a batch spanning shards is a two-phase commit and that cost is a measurement, not an opinion — set it only once the cost has been measured on the cluster in question, and run the consistency check while it is set. Startup warns whenever it is on. |
| `SYNC_VERIFY_INTERVAL` | How often to compare each table against its source, e.g. `1h`. Unset means never. |
| `SYNC_VERIFY_REPAIR` | `true` to also repair the differences the comparison finds, rather than only report them. |
| `SYNC_MYSQL_CHECKPOINT_INTERVAL` | How often the MySQL binlog position is recorded, e.g. `1s`. Default `200ms`; `0` records every transaction, which costs a round trip and an fsync on the target for each one. After an unclean stop, replication replays at most this interval. |
| `SYNC_MONITORING_RETENTION_DAYS` | How many days of monitoring history to keep. Default `30`; `0` keeps everything, and the log grows by a row per table per interval on the same volume as the replication state. |

## Contributing

We encourage all contributions to this repository! Please fork the repository or open an issue, make changes, and submit a pull request.
Note: All interactions here should conform to the [Code of Conduct](https://github.com/retail-ai-inc/sync/blob/main/CODE_OF_CONDUCT.md).

## Give a Star! ⭐

If you like or are using this project, please give it a **star**. Thanks!
