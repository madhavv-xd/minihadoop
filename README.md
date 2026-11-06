# minihadoop

A small HDFS-style distributed file system written in Go, built to learn how Hadoop works under the hood. Nodes talk over gRPC; the contract lives in `proto/minihadoop.proto`.

## Architecture

- **NameNode** (`:9000`): tracks DataNodes via `Register`/`Heartbeat`. Holds file → block → node metadata and hands out 3 live nodes per block (`AddBlock`). Files are write-once.
  - **Failure detection:** a node silent for 10s is marked dead. Its blocks are found and re-replicated onto other live nodes.
  - **Edit log:** every metadata change is appended to `/meta/editlog.jsonl` (fsynced) and replayed on startup, so the NameNode survives restarts.
- **DataNode** (`:9001`): registers and heartbeats every 3s. Stores blocks on disk under `DATA_DIR` with a CRC32 checksum, forwards writes down the replication pipeline, and copies blocks to other nodes when the NameNode asks (`ReplicateBlock`). Reads re-check the checksum, so corrupt blocks are never served or copied.
- **Client**: `put` splits a file into 1 MB blocks and sends each to the first node in its pipeline; `get` fetches the blocks back in order from live nodes.

## Running

```sh
docker compose up --build                     # 1 NameNode + 5 DataNodes
docker compose run --rm client put /files/test.bin test.bin
docker compose run --rm client get test.bin /files/out.bin
```

The client runs in a container so it can reach the DataNodes by their compose hostnames. `./testdata` is mounted at `/files`.

Each node has its own Docker volume, so data survives `restart`, `stop`/`start` and rebuilds. `docker compose down -v` wipes everything.

Env vars: DataNode `NAMENODE_ADDR`, `SELF_ADDR`, `PORT`, `DATA_DIR`; NameNode `EDITLOG_PATH` (default `/meta/editlog.jsonl`).

## Fault-tolerance demo

```sh
docker compose logs -f namenode               # terminal 1
docker compose stop datanode3                 # terminal 2
# ~10s later: "marked DEAD" -> "under-replicated" -> "re-replicated to dn-X"
docker compose restart namenode               # metadata replayed from the edit log
docker compose run --rm client get test.bin /files/out.bin   # still matches the original
```

## Tests

```sh
go test ./internal/namenode/ -v
```

Covers edit-log replay, torn last lines, under-replicated block detection, and refusing writes with fewer than 3 live nodes.

## Status

Working: registration, heartbeats, checksummed block storage, 3-way replicated writes, reads, dead-node detection, re-replication, edit log with crash-safe replay.

Next: a Coordinator (mini-YARN) with task slots, data-local scheduling and retries, then embedding blocks for distributed semantic search.

Not planned for now: rebalancing, removing extra replicas, block reports, HA NameNode.