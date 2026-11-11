walnutDB

A lightweight key-value storage engine built from scratch using Rust.

walnutDB is an experimental storage engine written in Rust to explore how databases manage data, handle persistence, recover from failures, and organize data on disk.

The project focuses on understanding low-level database internals, including Write-Ahead Logging (WAL), in-memory storage, SSTables, manifest management, and atomic counters.

🚀 Project Overview

walnutDB is designed around a simple key-value storage architecture.

It currently includes:

In-memory key-value storage using BTreeMap.
Write-Ahead Logging (WAL) for persistence.
SSTable generation for storing data on disk.
Manifest management for tracking SSTable file numbers.
WAL recovery during database initialization.
Tombstone markers for representing deleted records.
A basic TCP server for accepting client commands.
🏗️ Architecture
text
                 Client
                   |
                   v
              TCP Server
             127.0.0.1:9098
                   |
                   v
             Storage Engine
                   |
          +--------+--------+
          |                 |
          v                 v
       WAL Log           MemTable
     (wal.log)          (BTreeMap)
                            |
                     Capacity Threshold
                            |
                            v
                         SSTable
                            |
                            v
                     MANIFEST/
                            |
                  current.sst + SST files

🛠️ Core Components
1. Storage Engine

The storage engine manages in-memory data and coordinates persistence.

Implemented functionality:

Key-value insertion.
In-memory storage using BTreeMap.
Thread-safe data structures using RwLock.
Capacity tracking using AtomicUsize.
WAL integration.
Recovery of records from the WAL during startup.

2. Write-Ahead Log (WAL)

The WAL records incoming data before it is inserted into the in-memory table.

Each record contains:

text
+----------------+----------------+
| Key Length     | Value Length   |
| 4 bytes        | 4 bytes        |
+----------------+----------------+
| Key Data                        |
+---------------------------------+
| Value Data                      |
+---------------------------------+


The current implementation uses binary serialization to store key-value records.

WAL operations include:

Writing records before updating the MemTable.
Flushing records using sync_all().
Recovering records during startup.
Clearing the WAL after SSTable creation.
3. MemTable

The MemTable stores active key-value records in memory.

Implementation:

rust
BTreeMap<Vec<u8>, StorageValue>


Supported value states:

rust
pub enum StorageValue {
    Active(Bytes),
    Tombstone,
}


The BTreeMap maintains keys in sorted order.

4. SSTable (Sorted String Table)

SSTables are used to persist in-memory records to disk.

When the MemTable reaches its configured capacity, the storage engine creates an SSTable.

Example filenames:

text
MANIFEST/
├── current.sst
├── 000000_level000.sst
├── 000001_level001.sst
└── 000002_level002.sst


Each SSTable stores binary key-value records.

The implementation also supports tombstone markers for deleted records.

5. Manifest Management

The manifest component manages SSTable numbering.

It currently provides:

Loading the persisted SSTable counter.
Generating SSTable filenames.
Incrementing and persisting the counter.
Tracking the next SSTable filename.
🌐 TCP Interface

walnutDB currently exposes a basic TCP server.

Default address: 127.0.0.1:9098


Start the server:

cargo run


Connect using Netcat:

nc 127.0.0.1 9098


Insert a record:

SET name walnutDB


Expected response:
Inserted

📦 Project Structure
text
walnutDB/
│
├── src/
│   ├── main.rs
│   │
│   └── db/
│       ├── manifest.rs
│       ├── sstable.rs
│       └── storage_engine.rs
│
├── MANIFEST/
│   └── current.sst
│
├── wal.log
├── Cargo.toml
├── Cargo.lock
└── README.md

⚙️ Getting Started
Prerequisites
Rust
Cargo

Install Rust:
rustup install stable

Clone the repository
git clone https://github.com/Gnanendravarmak/walnutDb.git

Navigate to the project
cd walnutDb

Build the project
cargo build

Run the database
cargo run

🧰 Technologies Used
Rust – Core programming language.
BTreeMap – Ordered in-memory key-value storage.
Bytes – Byte-oriented data handling.
AtomicUsize / AtomicU64 – Atomic counters.
RwLock – Synchronization for shared data.
TCP – Client-server communication.
Filesystem APIs – Persistent storage and WAL management.


The main goals are to understand:

Database storage architecture.
Binary data serialization.
Memory management and ownership in Rust.
Atomic operations and concurrency.
File I/O and persistence.
Crash recovery.
Log-structured storage concepts.

👨‍💻 Author
Gnanendra Varma
GitHub: @Gnanendravarmak

walnutDB is a systems engineering project architected to demonstrate low-level database storage internals and Rust performance mechanics. While functionally complete for its intended scope, it is not designed for production workloads.