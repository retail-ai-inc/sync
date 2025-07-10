# Sync Data Platform UI Guide

Sync Data Platform provides an intuitive interface for managing and monitoring data synchronization tasks. This guide covers the core UI features and usage instructions.


## Login Interface
<img width="800" alt="image" src="https://github.com/user-attachments/assets/7437b947-e615-4308-8362-83cf76cb75dc" />


- **Authentication Methods**:
  - Admin account (default: `admin` / `admin`)
  - Google account (must be enabled in authentication settings)

## Main Features

### Dashboard
<img width="800" alt="image" src="https://github.com/user-attachments/assets/9a874651-0832-48ba-8cd3-1137e6da5afe" />

### Task Management
<img width="800" alt="image" src="https://github.com/user-attachments/assets/597c5987-3eea-4a0a-ad4d-39c7d4594868" />


- View task status and details
- Search for specific tasks
- Click "Add new sync task" to create a task
- Operate tasks (start/stop/monitor)

### Monitoring Dashboard
<img width="800" alt="image" src="https://github.com/user-attachments/assets/e3670689-5662-4509-937e-6758a6536cce" />
<img width="800" alt="image" src="https://github.com/user-attachments/assets/a38eed54-96ac-4bda-9191-29bc25cdef06" />

- **Key Metrics**:
  - Sync status and progress
  - Current latency
  - Rows synced today
  - Table row trend chart

## Data Backup
<img width="800" alt="image" src="https://github.com/user-attachments/assets/ed3df3e9-d139-4a5f-8f6d-4852e0f27784" />

- Click "Add new backup task" to create scheduled backups
- Configure backup settings (MongoDB/PostgreSQL, BSON/JSON/CSV format, query filters)
- Set destination (GCS path, file naming patterns)
- Operate tasks (start/stop/schedule/monitor execution)
- Support incremental and full backup modes
- Download compressed backup files (.tar.gz) from GCS storage

## System Settings

### Authentication Settings
<img width="800" alt="image" src="https://github.com/user-attachments/assets/7df4b816-86ae-456e-95e5-0c64a053737d" />

- Enable/disable Google login
- Upload OAuth configuration
- Restart prompt

