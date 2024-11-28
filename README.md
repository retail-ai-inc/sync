# MongoDB Sync

A Go-based tool to synchronize MongoDB data from a **MongoDB cluster** to a **standalone MongoDB instance**, supporting both initial and incremental synchronization with change stream monitoring.

![image](https://github.com/user-attachments/assets/ecf34f6b-c16e-43bb-81b5-f023154f630f)


## Features

- **Initial Sync**: Bulk synchronization of data from the MongoDB cluster to the standalone MongoDB instance.
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
export PROJECT_ID=$PROJECT_ID
gcloud builds submit --config=cloudbuild/staging/cloudbuild.yaml
```
