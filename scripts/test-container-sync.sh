#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

suffix=$$
network="ofdb-sync-test-$suffix"
kv_a="ofdb-kv-sync-a-$suffix"
kv_b="ofdb-kv-sync-b-$suffix"
sql_a="ofdb-sql-sync-a-$suffix"
sql_b="ofdb-sql-sync-b-$suffix"
kv_volume_a="ofdb-kv-sync-a-data-$suffix"
kv_volume_b="ofdb-kv-sync-b-data-$suffix"
sql_volume_a="ofdb-sql-sync-a-data-$suffix"
sql_volume_b="ofdb-sql-sync-b-data-$suffix"
kv_port=${KV_TEST_PORT:-18080}
sql_port=${SQL_TEST_PORT:-18081}

cleanup() {
    docker rm -f "$kv_a" "$kv_b" "$sql_a" "$sql_b" >/dev/null 2>&1 || true
    docker volume rm "$kv_volume_a" "$kv_volume_b" "$sql_volume_a" "$sql_volume_b" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
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

start_server() {
    name=$1
    image=$2
    port=$3
    volume=$4
    shift 4
    if [ "$image" = ofdb-kv ]; then
        container_port=8080
    else
        container_port=8081
    fi
    docker run -d --name "$name" --network "$network" -p "127.0.0.1:$port:$container_port" \
        -v "$volume:/data" "$image" --database /data/ofdb.redb \
        --address "0.0.0.0:$container_port" "$@" >/dev/null
}

cargo build --locked -p ofdb-kv-client-cli -p ofdb-sql-client-cli --features ofdb-kv-client-cli/remote,ofdb-sql-client-cli/remote

docker network create "$network" >/dev/null
for volume in "$kv_volume_a" "$kv_volume_b" "$sql_volume_a" "$sql_volume_b"; do
    docker volume create "$volume" >/dev/null
done

start_server "$kv_a" ofdb-kv "$kv_port" "$kv_volume_a" --sync-listen
start_server "$kv_b" ofdb-kv "$((kv_port + 1))" "$kv_volume_b" --sync-listen
kv_id_a=$(wait_for_node_id "$kv_a")
kv_id_b=$(wait_for_node_id "$kv_b")
target/debug/kv-client-cli --endpoint "http://127.0.0.1:$kv_port" set sync-probe --value '{"type":"text","value":"sync-ok"}'
docker rm -f "$kv_a" "$kv_b" >/dev/null
start_server "$kv_a" ofdb-kv "$kv_port" "$kv_volume_a" --sync-peer "$kv_id_b"
start_server "$kv_b" ofdb-kv "$((kv_port + 1))" "$kv_volume_b" --sync-peer "$kv_id_a"
[ "$(wait_for_node_id "$kv_a")" = "$kv_id_a" ]
[ "$(wait_for_node_id "$kv_b")" = "$kv_id_b" ]
attempt=0
while [ "$attempt" -lt 90 ]; do
    if target/debug/kv-client-cli --endpoint "http://127.0.0.1:$((kv_port + 1))" get sync-probe 2>/dev/null | grep -q 'sync-ok'; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done
[ "$attempt" -lt 90 ] || { printf 'KV container sync did not complete\n' >&2; exit 1; }
printf 'KV container sync passed\n'

docker rm -f "$kv_a" "$kv_b" >/dev/null
start_server "$sql_a" ofdb-sql "$sql_port" "$sql_volume_a" --sync-listen
start_server "$sql_b" ofdb-sql "$((sql_port + 1))" "$sql_volume_b" --sync-listen
sql_id_a=$(wait_for_node_id "$sql_a")
sql_id_b=$(wait_for_node_id "$sql_b")
target/debug/sql-client-cli --endpoint "http://127.0.0.1:$sql_port" --query "CREATE TABLE sync_probe (id UUID PRIMARY KEY, name TEXT); INSERT INTO sync_probe VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'sync-ok')" >/dev/null
docker rm -f "$sql_a" "$sql_b" >/dev/null
start_server "$sql_a" ofdb-sql "$sql_port" "$sql_volume_a" --sync-peer "$sql_id_b"
start_server "$sql_b" ofdb-sql "$((sql_port + 1))" "$sql_volume_b" --sync-peer "$sql_id_a"
[ "$(wait_for_node_id "$sql_a")" = "$sql_id_a" ]
[ "$(wait_for_node_id "$sql_b")" = "$sql_id_b" ]
attempt=0
while [ "$attempt" -lt 90 ]; do
    if target/debug/sql-client-cli --endpoint "http://127.0.0.1:$((sql_port + 1))" --query 'SELECT name FROM sync_probe' 2>/dev/null | grep -q 'sync-ok'; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done
[ "$attempt" -lt 90 ] || { printf 'SQL container sync did not complete\n' >&2; exit 1; }
printf 'SQL container sync passed\n'
