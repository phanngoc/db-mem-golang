// Package database provides a high-performance in-memory database
// built on top of lock-free skip lists.
package database

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"db-mem-golang/skiplist"
)

var (
	ErrCollectionExists    = errors.New("collection already exists")
	ErrCollectionNotFound  = errors.New("collection not found")
	ErrIndexExists         = errors.New("index already exists")
	ErrIndexNotFound       = errors.New("index not found")
	ErrInvalidKeyType      = errors.New("invalid key type")
	ErrTransactionAborted  = errors.New("transaction aborted")
	ErrOperationInProgress = errors.New("another operation is in progress")
)

// Collection represents a named data collection similar to a table
type Collection struct {
	name     string
	data     *skiplist.SkipList
	indexes  map[string]*Index
	indexMu  sync.RWMutex
	stats    CollectionStats
	writesMu sync.Mutex // For coordinating mass operations like clear
}

// CollectionStats tracks usage statistics for a collection
type CollectionStats struct {
	reads      atomic.Int64
	writes     atomic.Int64
	deletes    atomic.Int64
	lastAccess atomic.Int64
}

// Index represents a secondary index on a collection
type Index struct {
	name       string
	keyFunc    func(value interface{}) (skiplist.Key, error)
	collection *Collection
	data       *skiplist.SkipList
}

// Database is the main database structure
type Database struct {
	collections    map[string]*Collection
	mu             sync.RWMutex
	cleanupTicker  *time.Ticker
	cleanupDone    chan struct{}
	gcInterval     time.Duration
	ttlCheckActive bool
}

// NewDatabase creates a new in-memory database
func NewDatabase(options ...Option) *Database {
	db := &Database{
		collections: make(map[string]*Collection),
		gcInterval:  5 * time.Minute,
		cleanupDone: make(chan struct{}),
	}

	// Apply options
	for _, opt := range options {
		opt(db)
	}

	// Start TTL cleanup routine if enabled
	if db.ttlCheckActive {
		db.startCleanupRoutine()
	}

	return db
}

// Option defines a database configuration option
type Option func(*Database)

// WithGCInterval sets the garbage collection interval
func WithGCInterval(interval time.Duration) Option {
	return func(db *Database) {
		db.gcInterval = interval
	}
}

// WithTTLCleanup enables or disables automatic TTL cleanup
func WithTTLCleanup(enabled bool) Option {
	return func(db *Database) {
		db.ttlCheckActive = enabled
	}
}

// startCleanupRoutine starts the TTL cleanup goroutine
func (db *Database) startCleanupRoutine() {
	db.cleanupTicker = time.NewTicker(db.gcInterval)

	go func() {
		for {
			select {
			case <-db.cleanupTicker.C:
				db.runTTLCleanup()
			case <-db.cleanupDone:
				db.cleanupTicker.Stop()
				return
			}
		}
	}()
}

// runTTLCleanup performs TTL cleanup on all collections
func (db *Database) runTTLCleanup() {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, col := range db.collections {
		col.data.TTLCleanup()

		// Also clean up any indexes
		col.indexMu.RLock()
		for _, idx := range col.indexes {
			idx.data.TTLCleanup()
		}
		col.indexMu.RUnlock()
	}
}

// Close shuts down the database
func (db *Database) Close() {
	if db.cleanupTicker != nil {
		close(db.cleanupDone)
	}

	// Additional cleanup could be added here
}

// CreateCollection creates a new collection
func (db *Database) CreateCollection(name string) (*Collection, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.collections[name]; exists {
		return nil, ErrCollectionExists
	}

	col := &Collection{
		name:    name,
		data:    skiplist.NewSkipList(),
		indexes: make(map[string]*Index),
	}

	db.collections[name] = col
	return col, nil
}

// GetCollection returns a collection by name
func (db *Database) GetCollection(name string) (*Collection, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	col, exists := db.collections[name]
	if (!exists) {
		return nil, ErrCollectionNotFound
	}

	return col, nil
}

// DropCollection removes a collection
func (db *Database) DropCollection(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.collections[name]; !exists {
		return ErrCollectionNotFound
	}

	delete(db.collections, name)
	return nil
}

// ListCollections returns a list of all collection names
func (db *Database) ListCollections() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	collections := make([]string, 0, len(db.collections))
	for name := range db.collections {
		collections = append(collections, name)
	}

	return collections
}

// Stats returns database statistics
func (db *Database) Stats() map[string]interface{} {
	db.mu.RLock()
	defer db.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["collections"] = len(db.collections)

	colStats := make(map[string]interface{})
	for name, col := range db.collections {
		colStats[name] = map[string]interface{}{
			"count":      col.data.Len(),
			"reads":      col.stats.reads.Load(),
			"writes":     col.stats.writes.Load(),
			"deletes":    col.stats.deletes.Load(),
			"lastAccess": time.Unix(0, col.stats.lastAccess.Load()).Format(time.RFC3339),
			"indexes":    len(col.indexes),
		}
	}

	stats["collectionStats"] = colStats
	return stats
}

// Set sets a key-value pair in a collection
func (col *Collection) Set(key interface{}, value interface{}) error {
	k, err := skiplist.NewKey(key)
	if err != nil {
		return err
	}

	col.writesMu.Lock()
	defer col.writesMu.Unlock()

	// Update primary storage
	col.data.Set(k, value)

	// Update indexes
	col.indexMu.RLock()
	defer col.indexMu.RUnlock()

	for _, idx := range col.indexes {
		indexKey, err := idx.keyFunc(value)
		if err != nil {
			continue // Skip this index if we can't extract a key
		}

		idx.data.Set(indexKey, k) // Store the main key in the index
	}

	// Update stats
	col.stats.writes.Add(1)
	col.stats.lastAccess.Store(time.Now().UnixNano())

	return nil
}

// SetWithTTL sets a key-value pair with a time-to-live
func (col *Collection) SetWithTTL(key interface{}, value interface{}, ttlSeconds int64) error {
	k, err := skiplist.NewKey(key)
	if err != nil {
		return err
	}

	col.writesMu.Lock()
	defer col.writesMu.Unlock()

	// Update primary storage with TTL
	col.data.SetWithTTL(k, value, ttlSeconds)

	// Update indexes
	col.indexMu.RLock()
	defer col.indexMu.RUnlock()

	for _, idx := range col.indexes {
		indexKey, err := idx.keyFunc(value)
		if err != nil {
			continue // Skip this index if we can't extract a key
		}

		idx.data.SetWithTTL(indexKey, k, ttlSeconds) // Store the main key in the index with same TTL
	}

	// Update stats
	col.stats.writes.Add(1)
	col.stats.lastAccess.Store(time.Now().UnixNano())

	return nil
}

// Get retrieves a value by key
func (col *Collection) Get(key interface{}) (interface{}, bool) {
	k, err := skiplist.NewKey(key)
	if err != nil {
		return nil, false
	}

	// Get from primary storage
	val, found := col.data.GetWithTTLCheck(k)

	// Update stats
	col.stats.reads.Add(1)
	col.stats.lastAccess.Store(time.Now().UnixNano())

	return val, found
}

// Delete removes a key
func (col *Collection) Delete(key interface{}) bool {
	k, err := skiplist.NewKey(key)
	if err != nil {
		return false
	}

	col.writesMu.Lock()
	defer col.writesMu.Unlock()

	// Get the value before deleting (for index cleanup)
	val, found := col.data.Get(k)
	if !found {
		return false
	}

	// Delete from primary storage
	success := col.data.Delete(k)
	if !success {
		return false
	}

	// Delete from indexes
	col.indexMu.RLock()
	defer col.indexMu.RUnlock()

	for _, idx := range col.indexes {
		indexKey, err := idx.keyFunc(val)
		if err != nil {
			continue
		}
		idx.data.Delete(indexKey)
	}

	// Update stats
	col.stats.deletes.Add(1)
	col.stats.lastAccess.Store(time.Now().UnixNano())

	return true
}

// Clear removes all elements from the collection
func (col *Collection) Clear() {
	col.writesMu.Lock()
	defer col.writesMu.Unlock()

	// Clear primary storage
	col.data.Clear()

	// Clear all indexes
	col.indexMu.RLock()
	defer col.indexMu.RUnlock()

	for _, idx := range col.indexes {
		idx.data.Clear()
	}
}

// CreateIndex creates a new index on the collection
func (col *Collection) CreateIndex(name string, keyFunc func(interface{}) (skiplist.Key, error)) (*Index, error) {
	col.indexMu.Lock()
	defer col.indexMu.Unlock()

	if _, exists := col.indexes[name]; exists {
		return nil, ErrIndexExists
	}

	idx := &Index{
		name:       name,
		keyFunc:    keyFunc,
		collection: col,
		data:       skiplist.NewSkipList(),
	}

	col.indexes[name] = idx

	// Populate the index with existing data
	col.data.RangeQuery(skiplist.Key{}, skiplist.Key{}, func(k skiplist.Key, v interface{}) bool {
		indexKey, err := keyFunc(v)
		if err == nil {
			idx.data.Set(indexKey, k)
		}
		return true
	})

	return idx, nil
}

// GetIndex returns an index by name
func (col *Collection) GetIndex(name string) (*Index, error) {
	col.indexMu.RLock()
	defer col.indexMu.RUnlock()

	idx, exists := col.indexes[name]
	if !exists {
		return nil, ErrIndexNotFound
	}

	return idx, nil
}

// DropIndex removes an index
func (col *Collection) DropIndex(name string) error {
	col.indexMu.Lock()
	defer col.indexMu.Unlock()

	if _, exists := col.indexes[name]; !exists {
		return ErrIndexNotFound
	}

	delete(col.indexes, name)
	return nil
}

// Query finds values using an index
func (idx *Index) Query(indexKey interface{}) ([]interface{}, error) {
	k, err := skiplist.NewKey(indexKey)
	if err != nil {
		return nil, err
	}

	// Get the main key from the index
	mainKey, found := idx.data.GetWithTTLCheck(k)
	if !found {
		return nil, nil
	}

	// Use the main key to get the value
	originalKey, ok := mainKey.(skiplist.Key)
	if !ok {
		return nil, errors.New("invalid index reference")
	}

	value, found := idx.collection.data.GetWithTTLCheck(originalKey)
	if !found {
		return nil, nil
	}

	// Update stats
	idx.collection.stats.reads.Add(1)
	idx.collection.stats.lastAccess.Store(time.Now().UnixNano())

	return []interface{}{value}, nil
}

// RangeQuery performs a range query using an index
func (idx *Index) RangeQuery(startKey, endKey interface{}) ([]interface{}, error) {
	start, err := skiplist.NewKey(startKey)
	if err != nil {
		return nil, err
	}

	end, err := skiplist.NewKey(endKey)
	if err != nil {
		return nil, err
	}

	var results []interface{}

	idx.data.RangeQuery(start, end, func(k skiplist.Key, mainKeyVal interface{}) bool {
		originalKey, ok := mainKeyVal.(skiplist.Key)
		if !ok {
			return true
		}

		value, found := idx.collection.data.GetWithTTLCheck(originalKey)
		if found {
			results = append(results, value)
		}

		return true
	})

	// Update stats
	idx.collection.stats.reads.Add(1)
	idx.collection.stats.lastAccess.Store(time.Now().UnixNano())

	return results, nil
}

// Transaction represents a batch of operations
type Transaction struct {
	db        *Database
	col       *Collection
	ops       []skiplist.BatchOperation
	committed bool
	aborted   bool
	mu        sync.Mutex
}

// NewTransaction starts a new transaction on a collection
func (col *Collection) NewTransaction() *Transaction {
	return &Transaction{
		col: col,
		ops: make([]skiplist.BatchOperation, 0),
	}
}

// Set adds a set operation to the transaction
func (tx *Transaction) Set(key interface{}, value interface{}) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed || tx.aborted {
		return ErrTransactionAborted
	}

	k, err := skiplist.NewKey(key)
	if err != nil {
		return err
	}

	tx.ops = append(tx.ops, skiplist.BatchOperation{
		Key:      k,
		Value:    value,
		IsDelete: false,
	})

	return nil
}

// SetWithTTL adds a set operation with TTL to the transaction
func (tx *Transaction) SetWithTTL(key interface{}, value interface{}, ttlSeconds int64) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed || tx.aborted {
		return ErrTransactionAborted
	}

	k, err := skiplist.NewKey(key)
	if err != nil {
		return err
	}

	tx.ops = append(tx.ops, skiplist.BatchOperation{
		Key:      k,
		Value:    value,
		TTL:      ttlSeconds,
		IsDelete: false,
	})

	return nil
}

// Delete adds a delete operation to the transaction
func (tx *Transaction) Delete(key interface{}) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed || tx.aborted {
		return ErrTransactionAborted
	}

	k, err := skiplist.NewKey(key)
	if err != nil {
		return err
	}

	tx.ops = append(tx.ops, skiplist.BatchOperation{
		Key:      k,
		IsDelete: true,
	})

	return nil
}

// Commit executes all operations in the transaction
func (tx *Transaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed || tx.aborted {
		return ErrTransactionAborted
	}

	tx.col.writesMu.Lock()
	defer tx.col.writesMu.Unlock()

	// Execute batch operations on main data
	success := tx.col.data.ExecuteBatch(tx.ops)
	if (!success) {
		tx.aborted = true
		return ErrTransactionAborted
	}

	// Update indexes
	tx.col.indexMu.RLock()
	for _, op := range tx.ops {
		if !op.IsDelete {
			for _, idx := range tx.col.indexes {
				indexKey, err := idx.keyFunc(op.Value)
				if err != nil {
					continue
				}

				if op.TTL > 0 {
					idx.data.SetWithTTL(indexKey, op.Key, op.TTL)
				} else {
					idx.data.Set(indexKey, op.Key)
				}
			}
		} else {
			// For deletes, we need to find the value first to update indexes
			val, found := tx.col.data.Get(op.Key)
			if found {
				for _, idx := range tx.col.indexes {
					indexKey, err := idx.keyFunc(val)
					if err != nil {
						continue
					}
					idx.data.Delete(indexKey)
				}
			}
		}
	}
	tx.col.indexMu.RUnlock()

	// Update stats
	tx.col.stats.writes.Add(int64(len(tx.ops)))
	tx.col.stats.lastAccess.Store(time.Now().UnixNano())

	tx.committed = true
	return nil
}

// Abort cancels the transaction
func (tx *Transaction) Abort() {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.aborted = true
}

// Size returns the number of operations in the transaction
func (tx *Transaction) Size() int {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	return len(tx.ops)
}
