#!/bin/bash
set -e

# Start crond in background
if ! pgrep crond > /dev/null; then
    echo "Starting crond..."
    crond -f -d 8 > /var/log/cron.log 2>&1 &
    echo "Crond started with PID: $!"
else
    echo "Crond is already running"
fi

# Start the main application
echo "Starting sync application..."
exec ./sync 