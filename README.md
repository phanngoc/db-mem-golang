# Fast In-Memory Database with WAL Persistence

This project implements a high-performance in-memory database with lock-free skip lists and Write-Ahead Log (WAL) persistence.

## Features

- **Lock-Free Skip Lists**: High-performance concurrent data structure for in-memory storage
- **Write-Ahead Log (WAL)**: Persistence mechanism to recover data after restarts
- **Secondary Indexes**: Create and query data through secondary indexes
- **TTL Support**: Automatic expiration of data items
- **Transactions**: Batch multiple operations together

## WAL Implementation

The Write-Ahead Log (WAL) provides persistence for our in-memory database by recording all write operations before they are applied to the database. This ensures that in case of system failure or restart, all committed data can be recovered.

### Key Features of the WAL

1. **Durability**: Every write operation is recorded in the log before it's applied
2. **Atomicity**: Transaction support ensures operations are all applied or none
3. **Recovery**: Automatic recovery rebuilds database state from the log on startup
4. **Checkpoints**: Periodic checkpoints mark consistent states in the log
5. **Log Rotation**: Automatically rotates logs to prevent unbounded growth

## How to Use

### Enabling WAL

To enable WAL for your database:

```go
walOptions := wal.Options{
    Dir:                "data",               // Directory for log files
    SyncInterval:       200 * time.Millisecond, // How often to sync to disk
    CheckpointInterval: 5 * time.Minute,      // How often to create checkpoints
    MaxLogSize:         50 * 1024 * 1024,     // Max log size before rotation
}

db := database.NewDatabase(
    database.WithWAL(walOptions),         // Enable WAL
    database.WithRecovery(true),          // Enable recovery at startup
    database.WithTTLCleanup(true),        // Optional: enable TTL cleanup
)
```

### Testing Persistence

A test script is included to demonstrate persistence across restarts:

```bash
./test_persistence.sh
```

This script:
1. Runs the database, creating some data and recording it in the WAL
2. Restarts the database, which should recover the data from the WAL
3. Shows that data persists across restarts

## Data Model

The database uses a collections-based model, similar to NoSQL databases:

```go
// Create a collection
people, err := db.CreateCollection("people")

// Add data
people.Set(42, Person{ID: 42, Name: "Alice", Age: 28})

// Retrieve data
val, found := people.Get(42)
person := val.(Person)

// Create indexes
nameIndex, err := people.CreateIndex("name_idx", func(val interface{}) (skiplist.Key, error) {
    if person, ok := val.(Person); ok {
        return skiplist.NewKey(person.Name)
    }
    return skiplist.Key{}, fmt.Errorf("invalid type")
})

// Query by index
results, err := nameIndex.Query("Alice")
```

## Performance

The database is optimized for high concurrency with lock-free algorithms, supporting thousands of operations per second even with multiple concurrent readers and writers.

## Thread Safety

All operations in MemDB are thread-safe and designed for concurrent access. The skip list implementation uses atomic operations and hazard pointers to ensure safe memory reclamation, avoiding the common problems associated with lock-free data structures:

1. **ABA Problem**: Prevented via hazard pointers and memory retirement
2. **Memory Reclamation**: Safe memory management with hazard pointers
3. **Consistency**: Maintained through atomic operations and CAS

## Future Enhancements

Potential future enhancements include:

- Persistence layer with Write-Ahead Logging (WAL)
- Distributed consensus for multi-node deployments
- Additional query language or DSL
- Enhanced monitoring and metrics
- Snapshot isolation for more advanced transactions

## License

[MIT License](LICENSE)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.