# minihadoop

A small HDFS-style distributed file system written in Go, built to learn how Hadoop works under the hood. Nodes talk over gRPC; the contract lives in `proto/minihadoop.proto`.

## Architecture

- **NameNode** (`:9000`): tracks DataNodes via `Register`/`Heartbeat` (alive = seen in the last 10s). Holds file → block → node metadata in memory and hands out 3 nodes per block (`AddBlock`). Files are write-once.
- **DataNode** (`:9001`): registers and heartbeats every 3s. Stores blocks on disk under `DATA_DIR` with a CRC32 checksum, and forwards each write down the replication pipeline. Reads re-check the checksum.
- **Client**: `put` splits a file into 1 MB blocks and sends each to the first node in its pipeline; `get` fetches the blocks back in order.

## Running

```sh
docker compose up --build                     # 1 NameNode + 5 DataNodes
docker compose run --rm client put /files/test.bin test.bin
docker compose run --rm client get test.bin /files/out.bin
```

The client runs in a container so it can reach the DataNodes by their compose hostnames. `./testdata` is mounted at `/files`.

DataNode env vars: `NAMENODE_ADDR`, `SELF_ADDR`, `PORT`, `DATA_DIR`.

## Status

Working: registration, heartbeats, block storage with checksums, 3-way replicated writes, reads.

Not built yet: re-replication when a node dies, NameNode commands (delete/replicate), running tasks on DataNodes.
