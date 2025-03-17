# Sync Data Platform UI Guide

Sync Data Platform provides an intuitive interface for managing and monitoring data synchronization tasks. This guide covers the core UI features and usage instructions.

## Table of Contents
- [Login Interface](#login-interface)
- [Main Features](#main-features)
- [Operation Guide](#operation-guide)
- [SQL Debugging Tool](#sql-debugging-tool)
- [System Settings](#system-settings)
- [Monitoring and Logs](#monitoring-and-logs)

## Login Interface
![Login Interface](images/login.png)

- **Authentication Methods**:
  - Admin account (default: `admin` / `admin`)
  - Google account (must be enabled in authentication settings)

## Main Features

### Task Management
![Task Management Interface](images/task_list.png)

- View task status and details
- Search for specific tasks
- Click "Add new sync task" to create a task
- Operate tasks (start/stop/monitor)

### Monitoring Dashboard
![Monitoring Dashboard](images/dashboard.png)

- **Key Metrics**:
  - Sync status and progress
  - Current latency
  - Rows synced today
  - Table row trend chart

## Operation Guide

### Creating a Sync Task
1. On the task list page, click "Add new sync task"
2. Configure:
   - Data source type (MongoDB/MySQL/PostgreSQL/Redis)
   - Source and target database connections
   - Table/collection mappings
   - Sync options and security settings

### Monitoring Task Status
![Task Monitoring](images/task_monitor.png)

- Real-time status display
- Data trends (selectable time ranges)
- Source, target, and difference data comparison
- Auto-refresh

## SQL Debugging Tool
![SQL Debugging Tool](images/sql_debug.png)

- Select a sync task
- Query templates (Query/Insert/Update/Delete)
- Execute queries on source and target databases simultaneously
- Display result comparison

## System Settings

### Authentication Settings
![Authentication Settings](images/auth_settings.png)

- Enable/disable Google login
- Upload OAuth configuration
- Restart prompt

## Monitoring and Logs

### Log Management
![Log Management](images/log_management.png)

- Filter by level (INFO/WARN/ERROR/DEBUG)
- Log search
- Real-time refresh
- Timestamp and formatted messages