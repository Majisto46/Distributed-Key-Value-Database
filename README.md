# Distributed Key-Value Database

A Go-based distributed key-value database engine designed to demonstrate the core ideas behind modern storage systems: write-ahead logging, LSM-tree storage, Raft consensus, sharding, MVCC, and change-data capture. This project was built as a hands-on system design and distributed systems implementation exercise and is suitable for showcasing on a resume or in an interview.

## Project Summary

This project implements a sharded, fault-tolerant key-value store with:

- A custom storage engine based on WAL + MemTable + SSTables
- Log-structured merge compaction and Bloom filters
- Raft-based leader election and replicated logs
- Consistent hashing with virtual nodes for shard routing
- MVCC with snapshot isolation and conflict detection
- Smart client routing to the active leader
- CDC streaming for asynchronous event propagation

The codebase is organized to mirror real-world database architecture patterns, making it useful both as an educational project and as evidence of distributed systems engineering capability.

## Why This Project Matters

This system demonstrates practical knowledge across several core distributed systems topics:

- Storage engine internals and crash recovery
- Consensus and distributed coordination
- Data sharding and rebalancing
- Multi-version concurrency control
- Low-latency client routing
- Event-driven replication and stream subscriptions

For a resume, this project communicates strong experience in systems design, Go development, and distributed database internals.

## Core Architecture

### 1. Storage Engine

The storage layer implements a write-optimized design similar to production databases:

- WAL for durable writes and crash recovery
- MemTable for in-memory writes using an ordered structure
- SSTable persistence for immutable sorted files
- Bloom filters to avoid unnecessary reads
- Compaction to merge historical data efficiently

This produces a durable LSM-style engine optimized for write-heavy workloads.

### 2. Raft Consensus

A custom Raft layer provides:

- Leader election
- Log replication across peers
- Leader heartbeat handling
- Quorum-based correctness
- Recovery after leader failure

This ensures that writes are committed only after a majority of replicas agree.

### 3. Sharding and Consistent Hashing

The system routes keys using a consistent hash ring with virtual nodes:

- Efficient horizontal scaling across shards
- Minimal redistribution when nodes are added or removed
- Replica-aware routing for distributed workloads

### 4. MVCC and Snapshot Isolation

The MVCC layer supports:

- Versioned reads by timestamp
- Write conflict detection
- Transaction-style snapshot semantics
- Point-in-time reads and deletes

### 5. CDC and Smart Client

A smart routing client reduces latency by:

- Caching shard leader information
- Directly sending requests to the leader
- Retrying when leadership changes
- Subscribing to a CDC feed for real-time changes

## Repository Structure

```text
.
├── cmd/
│   └── cluster-sim/
│       └── main.go
├── docs/
│   └── INTERVIEW_GUIDE.md
├── pkg/
│   ├── cdc/
│   ├── client/
│   ├── consensus/
│   ├── mvcc/
│   ├── rpc/
│   ├── sharding/
│   └── storage/
├── go.mod
├── README.md
└── .gitignore
```

## Getting Started

### Prerequisites

- Go 1.27+ (as defined in the project module)

### Run all tests

```bash
go test ./...
```

### Run the cluster simulation demo

```bash
go run ./cmd/cluster-sim
```

This starts a demo cluster and exercises:

- Raft leader election
- Shard registration
- Smart client routing
- CDC publishing and subscription
- Distributed GET/PUT operations

## Example Resume-Friendly Bullet Points

- Designed and implemented a distributed key-value database in Go using Raft, consistent hashing, MVCC, and LSM-tree storage.
- Built a crash-safe WAL and SSTable-based storage layer with compaction and Bloom filters for efficient recovery and lookups.
- Engineered a sharded system with leader election, quorum replication, and smart client routing to minimize latency.
- Implemented MVCC snapshot isolation with optimistic conflict detection and point-in-time reads.
- Added CDC event streaming for real-time change propagation and subscriber replay.

## Notes

This project is intentionally implementation-focused and systems-oriented. It is meant to mirror real distributed database design decisions and serve as a strong technical portfolio project for software engineering, backend systems, infrastructure, or database roles.

## License

This project is provided as an educational and portfolio project. Add your preferred license if you plan to publish it publicly.
