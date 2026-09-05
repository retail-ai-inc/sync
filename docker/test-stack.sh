#!/usr/bin/env bash
# Brings up the databases the tagged tests talk to and prints the environment
# they read. The two Redis clusters need forming after the nodes start; a
# cluster that is only started serves no slots and every cluster test skips.
set -euo pipefail

here=$(dirname "$0")
sudo=""
[ "$(id -u)" -eq 0 ] || sudo="sudo"
compose="$sudo docker compose -f $here/docker-compose.yml"
test_compose="$sudo docker compose -f $here/docker-compose.test.yml"

# Only the services the harness addresses. The rest of the development stack
# binds ports these tests never use, and one of them collides with a local
# mongod often enough to stop this script before it starts anything.
$compose up -d mysql_source mysql_target postgresql_source postgresql_target
$test_compose up -d

# Slots are assigned once. Re-forming a live cluster fails, so only form one
# that has none.
form() {
  local container=$1 first=$2 second=$3 third=$4
  if $test_compose exec -T "$container" redis-cli -p "${first##*:}" cluster info 2>/dev/null |
       grep -q "cluster_state:ok"; then
    return 0
  fi
  $test_compose exec -T "$container" \
    redis-cli --cluster create "$first" "$second" "$third" --cluster-yes >/dev/null
}

form redis_cluster_1 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003
form redis_cluster_4 127.0.0.1:7004 127.0.0.1:7005 127.0.0.1:7006

cat <<'ENV'
export SYNC_REDIS_SOURCE=127.0.0.1:6479
export SYNC_REDIS_TARGET=127.0.0.1:6480
export SYNC_REDIS_SOURCE_CLUSTER=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
export SYNC_REDIS_TARGET_CLUSTER=127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006
export SYNC_REDIS_ALLOW_FLUSH=1
ENV
