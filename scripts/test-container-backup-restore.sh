#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

suffix=$$
network="ofdb-backup-test-$suffix"
backup_dir=$(mktemp -d)
containers="ofdb-backup-kv-$suffix ofdb-backup-kv-restored-$suffix ofdb-backup-sql-$suffix ofdb-backup-sql-restored-$suffix"
volumes="ofdb-backup-kv-source-$suffix ofdb-backup-kv-restore-$suffix ofdb-backup-sql-source-$suffix ofdb-backup-sql-restore-$suffix"

cleanup() {
    docker rm -f $containers >/dev/null 2>&1 || true
    docker volume rm $volumes >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
    rm -rf "$backup_dir"
}
trap cleanup EXIT INT TERM

wait_for_node_id() {
    container=$1
    attempt=0
    while [ "$attempt" -lt 60 ]; do
        node_id=$(docker logs "$container" 2>&1 | sed -n 's/.*sync node id: \([0-9a-f][0-9a-f]*\).*/\1/p' | head -n 1)
        if [ -n "$node_id" ]; then
            printf '%s\n' "$node_id"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    docker logs "$container" >&2
    printf 'server did not print its Iroh node ID: %s\n' "$container" >&2
    return 1
}

backup_volume() {
    volume=$1
    archive=$2
    docker run --rm -v "$volume:/data:ro" -v "$backup_dir:/backup" postgres:18-alpine \
        tar -C /data -czf "/backup/$archive" .
}

restore_volume() {
    volume=$1
    archive=$2
    docker run --rm -v "$volume:/data" -v "$backup_dir:/backup:ro" postgres:18-alpine \
        tar -C /data -xzf "/backup/$archive"
}

start_server() {
    name=$1
    image=$2
    volume=$3
    port=$4
    if [ "$image" = ofdb-kv ]; then
        container_port=8080
    else
        container_port=8081
    fi
    docker run -d --name "$name" --network "$network" -p "127.0.0.1:$port:$container_port" \
        -v "$volume:/data" "$image" --database /data/ofdb.redb \
        --address "0.0.0.0:$container_port" --sync-listen >/dev/null
}

cargo build --locked -p ofdb-kv-client-cli -p ofdb-sql-client-cli \
    --features ofdb-kv-client-cli/remote,ofdb-sql-client-cli/remote

docker network create "$network" >/dev/null
for volume in $volumes; do
    docker volume create "$volume" >/dev/null
done

kv_source="ofdb-backup-kv-$suffix"
kv_restored="ofdb-backup-kv-restored-$suffix"
sql_source="ofdb-backup-sql-$suffix"
sql_restored="ofdb-backup-sql-restored-$suffix"

start_server "$kv_source" ofdb-kv "ofdb-backup-kv-source-$suffix" 18090
kv_node_id=$(wait_for_node_id "$kv_source")
target/debug/kv-client-cli --endpoint http://127.0.0.1:18090 set backup-probe --value '{"type":"text","value":"backup-ok"}'
docker stop "$kv_source" >/dev/null
docker rm "$kv_source" >/dev/null
backup_volume "ofdb-backup-kv-source-$suffix" kv.tar.gz
restore_volume "ofdb-backup-kv-restore-$suffix" kv.tar.gz
start_server "$kv_restored" ofdb-kv "ofdb-backup-kv-restore-$suffix" 18090
[ "$(wait_for_node_id "$kv_restored")" = "$kv_node_id" ]
attempt=0
while [ "$attempt" -lt 30 ]; do
    if target/debug/kv-client-cli --endpoint http://127.0.0.1:18090 get backup-probe 2>/dev/null | grep -q backup-ok; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done
[ "$attempt" -lt 30 ] || { printf 'KV restore check failed\n' >&2; exit 1; }
printf 'KV backup and restore passed\n'
docker rm -f "$kv_restored" >/dev/null

start_server "$sql_source" ofdb-sql "ofdb-backup-sql-source-$suffix" 18091
sql_node_id=$(wait_for_node_id "$sql_source")
target/debug/sql-client-cli --endpoint http://127.0.0.1:18091 --query \
    "CREATE TABLE backup_probe (id UUID PRIMARY KEY, name TEXT); INSERT INTO backup_probe VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'backup-ok')" >/dev/null
docker stop "$sql_source" >/dev/null
docker rm "$sql_source" >/dev/null
backup_volume "ofdb-backup-sql-source-$suffix" sql.tar.gz
restore_volume "ofdb-backup-sql-restore-$suffix" sql.tar.gz
start_server "$sql_restored" ofdb-sql "ofdb-backup-sql-restore-$suffix" 18091
[ "$(wait_for_node_id "$sql_restored")" = "$sql_node_id" ]
attempt=0
while [ "$attempt" -lt 30 ]; do
    if target/debug/sql-client-cli --endpoint http://127.0.0.1:18091 --query \
        'SELECT name FROM backup_probe' 2>/dev/null | grep -q backup-ok; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done
[ "$attempt" -lt 30 ] || { printf 'SQL restore check failed\n' >&2; exit 1; }
printf 'SQL backup and restore passed\n'
