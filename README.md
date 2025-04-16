# MemDB: High-Performance In-Memory Database with Lock-Free Skip Lists

MemDB is a high-performance in-memory database built on lock-free skip lists, designed for concurrent environments where throughput and scalability are critical. By leveraging atomic operations instead of traditional locks, MemDB achieves exceptional performance even under heavy concurrency.

## Features

- **Lock-Free Architecture**: Uses atomic operations and Compare-And-Swap (CAS) instead of locks
- **Hazard Pointer Memory Management**: Safe memory reclamation for concurrent access
- **Persistent Skip List Structure**: Optimized for in-memory access patterns
- **Rich Query Capabilities**:
  - Key-value access
  - Range queries
  - Secondary indexes
- **Data Management**:
  - Time-To-Live (TTL) with automatic cleanup
  - Batch operations
  - Transaction support
- **Performance**:
  - Scales exceptionally well across multiple CPU cores
  - Achieves 900,000+ operations per second on standard hardware

## Architecture

MemDB is built on two main components:

1. **Skip List Core** (`skiplist` package):
   - Lock-free implementation with atomic operations
   - Hazard pointers for safe memory reclamation
   - Support for multiple key types (int, int64, string, []byte)
   - TTL support with efficient cleanup

2. **Database Layer** (`database` package):
   - Collections (similar to tables)
   - Secondary indexes
   - Transaction management
   - Statistics tracking

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/memdb.git

# Build the project
cd memdb
go build -o memdb-app
```

## Usage

### Basic Usage

```go
package main

import (
    "fmt"
    "time"
    "db-mem-golang/database"
)

func main() {
    // Create a new database
    db := database.NewDatabase(
        database.WithTTLCleanup(true),
        database.WithGCInterval(1*time.Minute),
    )
    defer db.Close()
    
    // Create a collection (similar to a table)
    users, err := db.CreateCollection("users")
    if err != nil {
        panic(err)
    }
    
    // Insert data
    users.Set("user1", map[string]interface{}{
        "name": "Alice",
        "email": "alice@example.com",
        "age": 28,
    })
    
    // Retrieve data
    userData, found := users.Get("user1")
    if found {
        fmt.Println("Found user:", userData)
    }
    
    // Set with expiration (TTL)
    users.SetWithTTL("temp_user", map[string]interface{}{
        "name": "Temporary",
        "role": "guest",
    }, 3600) // expires in 1 hour
}
```

### Creating Indexes

```go
// Create an index on the "name" field
nameIndex, err := users.CreateIndex("name_idx", func(val interface{}) (skiplist.Key, error) {
    if user, ok := val.(map[string]interface{}); ok {
        if name, ok := user["name"].(string); ok {
            return skiplist.NewKey(name)
        }
    }
    return skiplist.Key{}, fmt.Errorf("invalid user data for name index")
})

// Query using the index
results, err := nameIndex.Query("Alice")
if err == nil && len(results) > 0 {
    fmt.Println("Found users with name Alice:", results)
}

// Range query on an age index
youngUsers, err := ageIndex.RangeQuery(18, 30)
if err == nil {
    fmt.Println("Found young users:", youngUsers)
}
```

### Transactions

```go
// Start a transaction
tx := users.NewTransaction()

// Add operations to the transaction
tx.Set("user2", map[string]interface{}{"name": "Bob", "age": 32})
tx.Set("user3", map[string]interface{}{"name": "Charlie", "age": 45})
tx.Delete("old_user")

// Commit the transaction (all operations execute atomically)
if err := tx.Commit(); err != nil {
    fmt.Println("Transaction failed:", err)
} else {
    fmt.Println("Transaction committed successfully")
}
```

## Performance

The database is designed for high-throughput environments. In the included benchmark with 8 reader threads and 4 writer threads performing concurrent operations:

```
Running concurrent performance test...
Completed 120000 operations in 124.61725ms
Performance: 962949 operations/second
```

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