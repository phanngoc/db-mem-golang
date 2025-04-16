// Package wal provides a Write-Ahead Log implementation for
// persistent storage in the in-memory database.
package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultWalDir is the default directory for WAL files
	DefaultWalDir = "data"
	// DefaultWalFile is the default WAL file name
	DefaultWalFile = "wal.log"
	// EntryHeaderSize is the size of the entry header in bytes
	EntryHeaderSize = 8 // uint64 for entry size
	// DefaultSyncInterval is the default interval for syncing WAL to disk
	DefaultSyncInterval = 200 * time.Millisecond
	// DefaultCheckpointInterval is the default interval for creating checkpoints
	DefaultCheckpointInterval = 1 * time.Hour
	// DefaultMaxLogSize is the default maximum size for a WAL file before rotation
	DefaultMaxLogSize = 100 * 1024 * 1024 // 100MB
)

// WAL errors
var (
	ErrWALNotEnabled   = errors.New("WAL is not enabled")
	ErrWALCorrupted    = errors.New("WAL file is corrupted")
	ErrCheckpointError = errors.New("checkpoint creation failed")
)

// WAL is a Write-Ahead Log implementation
type WAL struct {
	// Directory for WAL files
	dir string
	// Current log file
	file *os.File
	// Buffer writer for better performance
	writer *bufio.Writer
	// Mutex for concurrent writes
	mu sync.Mutex
	// Current sequence number
	sequence uint64
	// Background sync ticker
	syncTicker *time.Ticker
	// Background checkpoint ticker
	checkpointTicker *time.Ticker
	// Signals to stop background operations
	done chan struct{}
	// Current log file size
	size int64
	// Max log file size before rotation
	maxLogSize int64
	// If true, WAL is enabled
	enabled bool
	// Function to be called during recovery
	recoveryHandler func(entry *Entry) error
}

// Options configures the WAL
type Options struct {
	// Directory for WAL files
	Dir string
	// Interval for syncing to disk
	SyncInterval time.Duration
	// Interval for creating checkpoints
	CheckpointInterval time.Duration
	// Maximum log file size before rotation
	MaxLogSize int64
	// If true, WAL is disabled (useful for testing)
	Disabled bool
}

// NewWAL creates a new WAL instance
func NewWAL(options Options) (*WAL, error) {
	if options.Disabled {
		return &WAL{enabled: false}, nil
	}

	// Set defaults if not provided
	if options.Dir == "" {
		options.Dir = DefaultWalDir
	}
	if options.SyncInterval <= 0 {
		options.SyncInterval = DefaultSyncInterval
	}
	if options.CheckpointInterval <= 0 {
		options.CheckpointInterval = DefaultCheckpointInterval
	}
	if options.MaxLogSize <= 0 {
		options.MaxLogSize = DefaultMaxLogSize
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(options.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	wal := &WAL{
		dir:        options.Dir,
		sequence:   0,
		done:       make(chan struct{}),
		maxLogSize: options.MaxLogSize,
		enabled:    true,
	}

	// Open the log file
	logPath := filepath.Join(options.Dir, DefaultWalFile)
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	wal.file = file

	// Get current file size
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get WAL file stats: %w", err)
	}
	wal.size = info.Size()

	// Create buffered writer
	wal.writer = bufio.NewWriter(file)

	// Start background sync
	wal.syncTicker = time.NewTicker(options.SyncInterval)
	go wal.syncRoutine()

	// Start background checkpoint
	wal.checkpointTicker = time.NewTicker(options.CheckpointInterval)
	go wal.checkpointRoutine()

	return wal, nil
}

// syncRoutine periodically syncs the WAL to disk
func (w *WAL) syncRoutine() {
	for {
		select {
		case <-w.syncTicker.C:
			w.mu.Lock()
			w.writer.Flush()
			w.file.Sync()
			w.mu.Unlock()
		case <-w.done:
			return
		}
	}
}

// checkpointRoutine periodically creates checkpoints
func (w *WAL) checkpointRoutine() {
	for {
		select {
		case <-w.checkpointTicker.C:
			w.Checkpoint()
		case <-w.done:
			return
		}
	}
}

// Write adds an entry to the WAL
func (w *WAL) Write(entry *Entry) error {
	if !w.enabled {
		return nil // WAL is disabled, silently succeed
	}

	// Set sequence number
	entry.Sequence = atomic.AddUint64(&w.sequence, 1)

	// Serialize the entry
	var entryBuf []byte
	{
		var buf []byte
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(entry); err != nil {
			return fmt.Errorf("failed to encode WAL entry: %w", err)
		}
		entryBuf = buf
	}

	// Prepare header (size of entry)
	header := make([]byte, EntryHeaderSize)
	binary.BigEndian.PutUint64(header, uint64(len(entryBuf)))

	w.mu.Lock()
	defer w.mu.Unlock()

	// Check if we need to rotate the log
	if w.size+int64(len(header)+len(entryBuf)) > w.maxLogSize {
		if err := w.rotate(); err != nil {
			return fmt.Errorf("failed to rotate WAL: %w", err)
		}
	}

	// Write header (entry size)
	n, err := w.writer.Write(header)
	if err != nil {
		return fmt.Errorf("failed to write WAL entry header: %w", err)
	}
	w.size += int64(n)

	// Write entry
	n, err = w.writer.Write(entryBuf)
	if err != nil {
		return fmt.Errorf("failed to write WAL entry: %w", err)
	}
	w.size += int64(n)

	return nil
}

// rotate creates a new log file and archives the old one
func (w *WAL) rotate() error {
	// Flush and sync the current file
	w.writer.Flush()
	w.file.Sync()

	// Close the current file
	oldPath := w.file.Name()
	w.file.Close()

	// Archive the current file
	timestamp := time.Now().Format("20060102-150405")
	archivePath := filepath.Join(w.dir, fmt.Sprintf("wal-%s.log", timestamp))
	if err := os.Rename(oldPath, archivePath); err != nil {
		return fmt.Errorf("failed to archive WAL file: %w", err)
	}

	// Create a new file
	file, err := os.OpenFile(oldPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create new WAL file: %w", err)
	}
	w.file = file
	w.writer = bufio.NewWriter(file)
	w.size = 0

	// Add a checkpoint entry to the new file
	checkpointEntry := NewCheckpointEntry()
	checkpointEntry.Sequence = atomic.AddUint64(&w.sequence, 1)

	var buf []byte
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(checkpointEntry); err != nil {
		return fmt.Errorf("failed to encode checkpoint entry: %w", err)
	}

	// Write header (entry size)
	header := make([]byte, EntryHeaderSize)
	binary.BigEndian.PutUint64(header, uint64(len(buf)))

	n, err := w.writer.Write(header)
	if err != nil {
		return fmt.Errorf("failed to write checkpoint header: %w", err)
	}
	w.size += int64(n)

	// Write entry
	n, err = w.writer.Write(buf)
	if err != nil {
		return fmt.Errorf("failed to write checkpoint entry: %w", err)
	}
	w.size += int64(n)

	return nil
}

// Checkpoint creates a checkpoint
func (w *WAL) Checkpoint() error {
	if !w.enabled {
		return ErrWALNotEnabled
	}

	// Log a checkpoint entry
	checkpointEntry := NewCheckpointEntry()
	return w.Write(checkpointEntry)
}

// Sync flushes and syncs the WAL to disk
func (w *WAL) Sync() error {
	if !w.enabled {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}
	return nil
}

// Close closes the WAL
func (w *WAL) Close() error {
	if !w.enabled {
		return nil
	}

	// Stop background operations
	close(w.done)
	w.syncTicker.Stop()
	w.checkpointTicker.Stop()

	// Final sync
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL on close: %w", err)
	}
	return w.file.Close()
}

// Recover reads the WAL and calls the recovery handler for each entry
func (w *WAL) Recover(handler func(entry *Entry) error) error {
	if !w.enabled {
		return ErrWALNotEnabled
	}

	// Store the recovery handler for future use
	w.recoveryHandler = handler

	// Find all log files
	pattern := filepath.Join(w.dir, "wal*.log")
	logFiles, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to list WAL files: %w", err)
	}

	// Sort log files by timestamp (archived files have timestamps in the name)
	// For simplicity, I'll keep the main file for last
	mainLog := filepath.Join(w.dir, DefaultWalFile)
	var archivedLogs []string
	for _, f := range logFiles {
		if f != mainLog {
			archivedLogs = append(archivedLogs, f)
		}
	}

	// First process archived logs, then the main log
	for _, logPath := range append(archivedLogs, mainLog) {
		if err := w.recoverFile(logPath, handler); err != nil {
			return err
		}
	}

	return nil
}

// recoverFile reads a single WAL file and applies each entry
func (w *WAL) recoverFile(path string, handler func(entry *Entry) error) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open WAL file for recovery: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var maxSequence uint64

	for {
		// Read entry header (size)
		header := make([]byte, EntryHeaderSize)
		_, err := io.ReadFull(reader, header)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read WAL entry header: %w", err)
		}

		// Decode entry size
		entrySize := binary.BigEndian.Uint64(header)

		// Read entry data
		entryBuf := make([]byte, entrySize)
		_, err = io.ReadFull(reader, entryBuf)
		if err != nil {
			return fmt.Errorf("failed to read WAL entry data: %w", err)
		}

		// Decode entry
		var entry Entry
		decoder := gob.NewDecoder(entryBuf)
		if err := decoder.Decode(&entry); err != nil {
			return fmt.Errorf("failed to decode WAL entry: %w", err)
		}

		// Track the highest sequence number
		if entry.Sequence > maxSequence {
			maxSequence = entry.Sequence
		}

		// Apply the entry
		if err := handler(&entry); err != nil {
			return fmt.Errorf("failed to apply WAL entry: %w", err)
		}
	}

	// Update our sequence number to be higher than any we've seen
	atomic.StoreUint64(&w.sequence, maxSequence)

	return nil
}
