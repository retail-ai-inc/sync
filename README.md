# MongoDB Sync: Synchronize Data from Replica Sets or Sharded Clusters to Standalone Instances

A Go-based tool to synchronize MongoDB data from a **MongoDB replica set** or **sharded cluster** to a **standalone MongoDB instance**, supporting both initial and incremental synchronization with change stream monitoring.

![image](https://github.com/user-attachments/assets/531d4b97-39d5-4c8c-a878-fd74d43b6d23)


## Features

- **Initial Sync**: Bulk synchronization of data from the MongoDB cluster or MongoDB replica set to the standalone MongoDB instance.
- **Incremental Sync**: Synchronizes newly updated or inserted data since the last sync using timestamps.
- **Change Stream Monitoring**: Watches for real-time changes (insert, update, replace, delete) in the cluster's collections and reflects them in the standalone instance.
- **Batch Processing**: Handles synchronization in batches for optimized performance.
- **Concurrent Execution**: Supports parallel synchronization for multiple collections.

## Prerequisites

- A source **MongoDB cluster** (replica set or sharded cluster).
- A target **standalone MongoDB instance** with write permissions.
- MongoDB version >= 4.0 for change stream support in the source cluster.
- Go 1.16 or later.

## Installation

```
# 1.Clone the repository:
git clone https://github.com/your-repo/mongodb-sync.git
cd mongodb-sync

# 2.Install dependencies
go mod tidy

# 3.Build the binary
cp config.json.local config.json
# Edit config.json to replace the placeholders with your MongoDB cluster and standalone instance details.
go build -o mongodb_sync .

# 4.Build the Docker image
docker build -t mongodb-sync .
docker run -v $(pwd)/config.json:/app/config.json mongodb-sync
```

### Configuration File: `config.json`

The `config.json` file defines the source (MongoDB replica set or sharded cluster) and the target (standalone MongoDB) for synchronization. Below is a detailed explanation of its structure and fields.

#### Fields Description

- **`cluster_a_uri`**:
  - This is the MongoDB URI for the **source** database. It must point to a replica set or sharded cluster.
- **`standalone_b_uri`**:
  - This is the MongoDB URI for the **target** standalone database.
- **`sync_mappings`**:
  - Defines how data flows from the source to the target. It is an array where each entry represents a mapping between a source database and a target database.
  - **`source_database`**: Name of the database in the source MongoDB cluster.
  - **`target_database`**: Name of the corresponding database in the target MongoDB.
  - **`collections`**: Defines the mapping of collections.
    - **`source_collection`**: Collection name in the source database.
    - **`target_collection`**: Corresponding collection name in the target database.

#### Example `config.json`

```json
{
  "cluster_a_uri": "mongodb://username:password@host1,host2,host3/?replicaSet=myReplicaSet",
  "standalone_b_uri": "mongodb://username:password@127.0.0.1:27017",
  "sync_mappings": [
    {
      "source_database": "source_db_1",
      "target_database": "target_db_1",
      "collections": [
        {
          "source_collection": "collection_a",
          "target_collection": "collection_a"
        },
        {
          "source_collection": "collection_b",
          "target_collection": "collection_b"
        }
      ]
    },
    {
      "source_database": "source_db_2",
      "target_database": "target_db_2",
      "collections": [
        {
          "source_collection": "collection_x",
          "target_collection": "collection_x"
        },
        {
          "source_collection": "collection_y",
          "target_collection": "collection_y"
        }
      ]
    }
  ]
}
```

## Real-Time Synchronization
This tool uses MongoDB Change Streams to capture real-time updates (inserts, updates, deletes) from the source replica set or sharded cluster and applies them to the target standalone instance.

## Availability  

Change streams are available for [replica sets and sharded clusters](https://www.mongodb.com/docs/manual/changeStreams/#availability).

If you are using a standalone MongoDB instance, it must be converted to a replica set. See [Convert Standalone to Replica Set](https://www.mongodb.com/docs/manual/tutorial/convert-standalone-to-replica-set/).

## Contributing
Contributions are welcome! Please fork the repository, make changes, and submit a pull request.