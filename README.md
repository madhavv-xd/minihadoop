# minihadoop

A small HDFS-style distributed file system written in Go, built to learn how Hadoop works under the hood. Nodes talk to each other over gRPC, and the contract lives in `proto/minihadoop.proto`.

## Architecture

- **NameNode** (`:9000`): keeps the table of DataNodes. It handles `Register` and `Heartbeat` calls and every 5s logs which nodes are alive (a node counts as alive if it was seen in the last 10s).
- **DataNode** (`:9001`): registers with the NameNode, retrying until that works, then sends a heartbeat every 3s. If the NameNode has forgotten it, the DataNode registers again. It also runs its own gRPC server for block operations.

## Running

```sh
docker compose up --build
```

This starts one NameNode and three DataNodes. Watch the NameNode logs to see the nodes register and stay alive.

DataNode config comes from env vars: `NAMENODE_ADDR`, `SELF_ADDR` and `PORT`.

## Status

Working now: node registration and heartbeats (Day 1).

Defined in the proto but not built yet: block storage and reads with CRC32 checksums, the replication write pipeline, NameNode commands (delete/replicate blocks), and running tasks on DataNodes.
