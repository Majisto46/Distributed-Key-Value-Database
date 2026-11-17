# Distributed Key-Value Database: Interview Mastery & Deep-Dive Guide

This guide is designed for high-stakes technical interviews (Staff / Senior Distributed Systems Engineer). It arms you with first-principles explanations, architectural diagrams, trade-off analysis, and concrete answers for every bullet point on your resume.

---

## 1. Resume Bullet 1: Custom Storage Engine (LSM-Tree & WAL)

> **Resume Line**: *Engineered a linearizable, sharded distributed database in Go, implementing a custom storage engine with Write-Ahead Logging (WAL) and LSM-trees for deterministic crash recovery.*

### Q1: Why did you choose an LSM-Tree over a B+ Tree?
**The Engineering Answer**:
- **Hardware Physics**: Random writes are expensive on both HDDs (mechanical head seeks) and modern NVMe SSDs (where modifying a single 100-byte record in-place requires reading, erasing, and rewriting an entire 4KB-16KB flash memory block, causing severe **Write Amplification** and wearing down SSD cells).
- **In-place vs Append-only**: B+ Trees modify pages in-place. Under heavy write workloads (e.g. 50k writes/sec), page locking and random page flushing saturate disk I/O.
- **LSM-Tree Advantage**: An LSM-tree turns all random writes into strictly sequential append operations:
  1. Append to Write-Ahead Log (WAL) on disk.
  2. Insert into in-memory MemTable (SkipList).
- **The Trade-Off (RUM Conjecture)**:
  LSM-trees optimize aggressively for **Write throughput** and **Space efficiency** (sequential packing with zero internal fragmentation). The cost is paid in **Read Amplification** (having to search MemTable + multiple SSTables), which we mitigate using **Bloom Filters** and **Sparse Block Indexes**.

### Q2: How does your WAL achieve deterministic crash recovery, and how do you handle torn writes?
**The Engineering Answer**:
- **Binary Record Layout**:
  ```
  +-----------------+-----------------+------------------+------------------+------------------+---------------+---------------+
  | CRC32 (4 Bytes) | Timestamp (8 B) | RecordType (1 B) | KeyLength (4 B)  | ValLength (4 B)  | Key (N Bytes) | Value (M B)   |
  +-----------------+-----------------+------------------+------------------+------------------+---------------+---------------+
  ```
- **Torn Write Protection**: If power cuts out mid-write, the disk might only flush the first 12 bytes of a 50-byte record. During crash recovery:
  1. We read the 21-byte header.
  2. We read `KeyLength + ValLength` bytes.
  3. We calculate `crc32.ChecksumIEEE(payload)`.
  4. If an unexpected EOF or CRC mismatch occurs at the end of the log file, we know an unclean shutdown severed the record.
  5. The recovery engine recovers all prior valid entries (100% verified by CRC32) and truncates the torn write, preventing corrupt data from entering the database.

### Q3: Why did you use a SkipList for the MemTable instead of a Red-Black Tree?
**The Engineering Answer**:
- In high-concurrency engines, balanced binary search trees (Red-Black / AVL) require tree rotations when inserting elements. A rotation near the root locks large portions of the tree, creating severe lock contention across concurrent writer goroutines.
- William Pugh's **SkipList** is a probabilistic multi-level linked list. Inserting a new node only requires splicing pointers at each level the node reaches. No tree rebalancing or rotations occur.
- Furthermore, ordered sequential range scans (`Seek`, `Next`) simply walk level 0 of the SkipList without any tree traversal overhead.

### Q4: How does the SSTable format avoid reading unnecessary disk blocks?
**The Engineering Answer**:
- Each `.sst` file contains:
  1. **Data Blocks** (4KB each, containing sorted records).
  2. **Sparse Block Index** (mapping `FirstKeyOfBlock -> {Offset, Length}`).
  3. **Bloom Filter** (probabilistic bit array).
  4. **40-Byte Footer** at EOF with metadata offsets and magic number `0x535354424C563031`.
- When searching for a key:
  1. **Step 1 (RAM)**: Check Bloom Filter. If it returns false, the key is guaranteed not in the file. **0 disk seeks!**
  2. **Step 2 (RAM)**: Binary search the sparse index to find the candidate block.
  3. **Step 3 (Disk)**: Perform **exactly one sequential disk read** to fetch that specific 4KB data block into memory and locate the key.

---

## 2. Resume Bullet 2: Consensus (Raft) & Consistent Hashing

> **Resume Line**: *Designed a highly available architecture utilizing a custom Raft consensus implementation for leader election and log replication, coupled with consistent hashing for dynamic shard rebalancing.*

### Q5: Walk me through a Raft election. How do you prevent split votes?
**The Engineering Answer**:
- When a follower's randomized election timer (150ms-300ms) expires without receiving a heartbeat:
  1. Increments `currentTerm++`.
  2. Transitions to `Candidate`.
  3. Votes for itself and saves persistent state (`term`, `votedFor`, `log`).
  4. Sends `RequestVote` RPCs in parallel to all peers.
- **Split-Vote Prevention**:
  If all nodes had identical timeouts, they would transition to Candidate simultaneously and split votes evenly. By randomizing election timeouts, one node almost always times out first, broadcasts `RequestVote`, and gathers a majority before other nodes time out.

### Q6: What are the two conditions for a voter to grant a vote in `RequestVote`?
**The Engineering Answer** (Raft Paper §5.4.1):
1. Candidate's `term >= currentTerm` and voter has either not voted (`votedFor == ""`) or already voted for this candidate.
2. Candidate's log is **at least as up-to-date as receiver's log**:
   - `candidate.LastLogTerm > receiver.LastLogTerm`
   - OR if terms are equal: `candidate.LastLogIndex >= receiver.LastLogIndex`.
*This Leader Completeness invariant guarantees that a candidate with missing committed entries can never win an election!*

### Q7: Can a leader commit an entry from a prior term by counting replicas?
**The Engineering Answer** (Diego Ongaro's Figure 8):
- **NO!** A Raft leader can **never** commit an entry from an older term simply by counting replicas.
- If a leader attempts to commit an older entry by replica counting, a former leader with higher term can overwrite it after reconnecting, violating state machine safety.
- A leader only commits log entries from its **current term** by counting replicas. When an entry from the current term reaches a majority, all prior entries are committed indirectly!

### Q8: Why did you use Consistent Hashing with Virtual Nodes?
**The Engineering Answer**:
- **The Modulo Problem**: Modulo hashing ($\text{Hash}(K) \pmod N$) reshuffles $\approx \frac{N}{N+1}$ (e.g. 91% when scaling from 10 to 11 nodes) of all keys across the entire database.
- **Consistent Hashing**: Keys and nodes are mapped onto a circular $2^{64}$ token ring. Adding a node only moves $\frac{1}{N+1}$ (e.g. 25% when scaling 3 to 4 nodes) of keys, and all moved keys go exclusively to the new node.
- **Virtual Nodes (vnodes = 128)**: Physical nodes placed directly on the ring cause severe hot-spots due to non-uniform spacing. By creating 128 virtual tokens per physical node with SHA-256 hashing, token distribution across the ring achieves uniform balance with $< 3\%$ variance.

---

## 3. Resume Bullet 3: MVCC, Snapshot Isolation & Smart Client Routing

> **Resume Line**: *Built a low-latency gRPC networking layer featuring smart client routing, Multi-Version Concurrency Control (MVCC) for snapshot isolation, and a CDC stream for asynchronous event publishing.*

### Q9: How does MVCC key encoding work in your storage engine?
**The Engineering Answer**:
- A user key and timestamp are encoded into a memcomparable format:
  ```
  [UserKey bytes (0x00 escaped as 0x00 0xFF)] [0x00 0x00 Delimiter] [8 Bytes Big-Endian: (MaxUint64 - ts)]
  ```
- **Why inverted timestamp (`MaxUint64 - ts`)?**
  In Big-Endian binary comparison, smaller values sort first. Because `MaxUint64 - 200 < MaxUint64 - 100`, the newer version ($TS = 200$) sorts **immediately before** older versions ($TS = 100$).
- When seeking for `userKey` at `readTS`, `Seek(EncodeKey(userKey, readTS))` lands directly on the newest visible committed version in $O(\log N)$ time with zero scanning overhead.

### Q10: How does your engine detect write conflicts under Snapshot Isolation?
**The Engineering Answer**:
- Under Snapshot Isolation, we enforce **First-Committer-Wins (Optimistic Concurrency Control)**:
  1. Transaction begins at snapshot timestamp $T_{read}$.
  2. Transaction buffers local puts and deletes.
  3. At commit time, the transaction acquires the global commit lock.
  4. For every key in the write set, it checks if any other transaction committed that key with timestamp $T_{commit} > T_{read}$.
  5. If a newer commit is detected: the transaction aborts with `ErrWriteConflict`!
  6. If clean: a monotonic commit timestamp is assigned, mutations are written, and the transaction succeeds.

### Q11: What makes your client "smart" compared to a standard database client?
**The Engineering Answer**:
- **Traditional Gateway Architecture**: Client -> Gateway/Load Balancer -> Internal Shard Routing -> Leader -> Storage.
  - Adds 2-3 extra network hops per query.
- **Smart Client Architecture**:
  1. Client maintains a local in-memory cache of the consistent hash ring and per-shard Raft leaders.
  2. When calling `client.Put(key, value)`, the client calculates the owning shard and dispatches the RPC **directly to the shard's Raft leader**.
  3. Latency on the happy path is reduced to a single direct round-trip ($0$ intermediary hops).
  4. If a leader election occurs, the contacted node returns `NotLeaderError`. The smart client invalidates its cache, rediscovers the new leader, and transparently retries.

### Q12: How does your Change Data Capture (CDC) stream work?
**The Engineering Answer**:
- When the Raft state machine commits and applies a mutation to the LSM/MVCC engine:
  - An event payload (`Sequence`, `Timestamp`, `Type`, `Key`, `Value`) is published to an asynchronous `cdc.Broadcaster`.
  - The broadcaster maintains a ring buffer of recent events and fans out to active gRPC streaming subscribers.
  - Clients can reconnect and pass `fromSequence` to seamlessly resume from where they left off without missing events.

