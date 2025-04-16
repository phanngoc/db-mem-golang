// Package wal provides a Write-Ahead Log implementation for
// persistent storage in the in-memory database.
package wal

import (
	"bytes"
	"db-mem-golang/skiplist"
	"encoding/gob"
	"time"
)

// OperationType defines the type of database operation
type OperationType byte

const (
	// OpSet represents a set operation
	OpSet OperationType = iota
	// OpDelete represents a delete operation
	OpDelete
	// OpClear represents a clear operation
	OpClear
	// OpCheckpoint represents a checkpoint operation
	OpCheckpoint
)

// Entry represents a single WAL log entry
type Entry struct {
	// Sequence number to maintain order
	Sequence uint64
	// Timestamp of the operation
	Timestamp int64
	// Collection is the name of the collection
	Collection string
	// Type of operation (set, delete, etc.)
	Type OperationType
	// Key for the operation
	Key []byte
	// Value for set operations
	Value []byte
	// TTL in seconds (0 means no TTL)
	TTL int64
}

// NewSetEntry creates a log entry for a set operation
func NewSetEntry(collection string, key skiplist.Key, value interface{}, ttl int64) (*Entry, error) {
	// Serialize the value
	var valueBuf bytes.Buffer
	encoder := gob.NewEncoder(&valueBuf)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}

	return &Entry{
		Timestamp:  time.Now().UnixNano(),
		Collection: collection,
		Type:       OpSet,
		Key:        key.ToBytes(),
		Value:      valueBuf.Bytes(),
		TTL:        ttl,
	}, nil
}

// NewDeleteEntry creates a log entry for a delete operation
func NewDeleteEntry(collection string, key skiplist.Key) *Entry {
	return &Entry{
		Timestamp:  time.Now().UnixNano(),
		Collection: collection,
		Type:       OpDelete,
		Key:        key.ToBytes(),
	}
}

// NewClearEntry creates a log entry for clearing a collection
func NewClearEntry(collection string) *Entry {
	return &Entry{
		Timestamp:  time.Now().UnixNano(),
		Collection: collection,
		Type:       OpClear,
	}
}

// NewCheckpointEntry creates a log entry for a checkpoint operation
func NewCheckpointEntry() *Entry {
	return &Entry{
		Timestamp: time.Now().UnixNano(),
		Type:      OpCheckpoint,
	}
}

// DecodeValue decodes the value from a log entry
func (e *Entry) DecodeValue(result interface{}) error {
	decoder := gob.NewDecoder(bytes.NewReader(e.Value))
	return decoder.Decode(result)
}

// GetValueAsInterface returns the value as an empty interface
func (e *Entry) GetValueAsInterface() (interface{}, error) {
	var value interface{}
	err := gob.NewDecoder(bytes.NewReader(e.Value)).Decode(&value)
	return value, err
}
