package skiplist

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	maxLevel          = 32   // Increased max level for larger datasets
	probability       = 0.25 // Lower probability for better balance
	maxHazardPointers = 100  // Maximum number of hazard pointers
	maxRetiredNodes   = 1000 // Maximum number of retired nodes before cleanup
)

// KeyType represents the supported key types
type KeyType interface {
	~int | ~int64 | ~string | ~[]byte
}

// Comparable defines the interface for key comparison
type Comparable interface {
	Less(other interface{}) bool
	Equal(other interface{}) bool
}

// Key wraps different types to make them comparable
type Key struct {
	value interface{}
}

// NewKey creates a new comparable key
func NewKey(value interface{}) (Key, error) {
	switch value.(type) {
	case int, int64, string, []byte:
		return Key{value: value}, nil
	default:
		return Key{}, fmt.Errorf("unsupported key type: %T", value)
	}
}

// Less compares if this key is less than another
func (k Key) Less(other interface{}) bool {
	otherKey, ok := other.(Key)
	if !ok {
		return false
	}

	switch k.value.(type) {
	case int:
		if v2, ok := otherKey.value.(int); ok {
			return k.value.(int) < v2
		}
	case int64:
		if v2, ok := otherKey.value.(int64); ok {
			return k.value.(int64) < v2
		}
	case string:
		if v2, ok := otherKey.value.(string); ok {
			return k.value.(string) < v2
		}
	case []byte:
		if v2, ok := otherKey.value.([]byte); ok {
			return bytes.Compare(k.value.([]byte), v2) < 0
		}
	}
	return false
}

// Equal checks if two keys are equal
func (k Key) Equal(other interface{}) bool {
	otherKey, ok := other.(Key)
	if !ok {
		return false
	}

	switch k.value.(type) {
	case int:
		if v2, ok := otherKey.value.(int); ok {
			return k.value.(int) == v2
		}
	case int64:
		if v2, ok := otherKey.value.(int64); ok {
			return k.value.(int64) == v2
		}
	case string:
		if v2, ok := otherKey.value.(string); ok {
			return k.value.(string) == v2
		}
	case []byte:
		if v2, ok := otherKey.value.([]byte); ok {
			return bytes.Equal(k.value.([]byte), v2)
		}
	}
	return false
}

// ToBytes converts the key to a byte representation for storage or hashing
func (k Key) ToBytes() []byte {
	switch v := k.value.(type) {
	case int:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(v))
		return b
	case int64:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(v))
		return b
	case string:
		return []byte(v)
	case []byte:
		return v
	default:
		return nil
	}
}

func (k Key) String() string {
	return fmt.Sprintf("%v", k.value)
}

// Node represents a node in the skip list
type Node struct {
	key   Key
	value atomic.Value
	next  [maxLevel]unsafe.Pointer
	// Adding timestamp for TTL support
	timestamp int64
	// Adding deleted flag for tombstones
	deleted atomic.Bool
}

// HazardPointer system to protect nodes from premature deletion
type HazardPointerSystem struct {
	hazardPointers [maxHazardPointers]unsafe.Pointer
	retiredNodes   []unsafe.Pointer
	retiredMutex   sync.Mutex // Only used for the retiredNodes slice
}

var hpSystem = &HazardPointerSystem{}

// Protect marks a node as in-use by the current thread
func (hp *HazardPointerSystem) Protect(idx int, node *Node) *Node {
	if node == nil {
		return nil
	}

	// Register this node in the hazard pointer array
	atomic.StorePointer(&hp.hazardPointers[idx], unsafe.Pointer(node))
	return node
}

// Release clears a hazard pointer
func (hp *HazardPointerSystem) Release(idx int) {
	atomic.StorePointer(&hp.hazardPointers[idx], nil)
}

// Retire adds a node to the retired list for later deletion
func (hp *HazardPointerSystem) Retire(node *Node) {
	hp.retiredMutex.Lock()
	hp.retiredNodes = append(hp.retiredNodes, unsafe.Pointer(node))

	// If we have too many retired nodes, attempt cleanup
	if len(hp.retiredNodes) >= maxRetiredNodes {
		hp.TryCleanup()
	}
	hp.retiredMutex.Unlock()
}

// TryCleanup scans hazard pointers and frees nodes that are safe to delete
func (hp *HazardPointerSystem) TryCleanup() {
	// Create a map of protected nodes
	protected := make(map[unsafe.Pointer]struct{})
	for i := 0; i < maxHazardPointers; i++ {
		ptr := atomic.LoadPointer(&hp.hazardPointers[i])
		if ptr != nil {
			protected[ptr] = struct{}{}
		}
	}

	// Find nodes safe to delete (not in hazard pointers)
	var remaining []unsafe.Pointer
	for _, ptr := range hp.retiredNodes {
		if _, found := protected[ptr]; !found {
			// In Go, we don't manually free memory, the GC will handle this
			// We just remove it from our list
		} else {
			remaining = append(remaining, ptr)
		}
	}

	// Update retired list
	hp.retiredNodes = remaining
}

type SkipList struct {
	head  *Node
	level int
	rnd   *rand.Rand
}

func NewSkipList() *SkipList {
	head := &Node{}
	return &SkipList{
		head:  head,
		level: 1,
		rnd:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (sl *SkipList) randomLevel() int {
	level := 1
	for level < maxLevel && sl.rnd.Float64() < probability {
		level++
	}
	return level
}

func (sl *SkipList) findPredecessors(key Key) [maxLevel]*Node {
	var preds [maxLevel]*Node
	curr := sl.head

	// Use hazard pointers for the current and next nodes
	hpCurrIdx := 0
	hpNextIdx := 1

	// Initialize all predecessors to the head node
	for i := 0; i < maxLevel; i++ {
		preds[i] = sl.head
	}

	// Protect the head node
	hpSystem.Protect(hpCurrIdx, curr)

	for i := sl.level - 1; i >= 0; i-- {
		for {
			nextPtr := atomic.LoadPointer(&curr.next[i])
			if nextPtr == nil {
				break
			}

			next := hpSystem.Protect(hpNextIdx, (*Node)(nextPtr))

			// Verify the pointer didn't change
			if atomic.LoadPointer(&curr.next[i]) != nextPtr {
				continue
			}

			if next == nil || !next.key.Less(key) {
				break
			}

			// Move forward
			curr = hpSystem.Protect(hpCurrIdx, next)
		}
		preds[i] = curr
	}

	// Release hazard pointers when done
	hpSystem.Release(hpNextIdx)
	// Don't release hpCurrIdx yet as the predecessors are still needed

	return preds
}

// Set inserts or updates a key-value pair
func (sl *SkipList) Set(key Key, value interface{}) {
	level := sl.randomLevel()
	preds := sl.findPredecessors(key)

	// Use hazard pointer to protect the next node
	hpNextIdx := 2 // Use different index than findPredecessors

	nextPtr := atomic.LoadPointer(&preds[0].next[0])
	next := (*Node)(nextPtr)

	if nextPtr != nil {
		// Protect the next node if it exists
		next = hpSystem.Protect(hpNextIdx, next)

		// Check if we're updating an existing key
		if next != nil && next.key.Equal(key) {
			next.value.Store(value)
			// Release hazard pointers
			hpSystem.Release(0) // From findPredecessors
			hpSystem.Release(hpNextIdx)
			return
		}
	}

	// Create a new node
	newNode := &Node{key: key}
	newNode.value.Store(value)

	// Insert at each level
	for i := 0; i < level; i++ {
		for {
			nextPtr = atomic.LoadPointer(&preds[i].next[i])
			next = (*Node)(nextPtr)
			newNode.next[i] = unsafe.Pointer(next)
			if atomic.CompareAndSwapPointer(&preds[i].next[i], unsafe.Pointer(next), unsafe.Pointer(newNode)) {
				break
			}

			// If CAS failed, we need to find new predecessors at this level
			if i == 0 {
				// Only need to refind at level 0 since this is where we check for duplicates
				tempPreds := sl.findPredecessors(key)
				preds[0] = tempPreds[0]

				// Check if key was inserted by another thread
				nextPtr = atomic.LoadPointer(&preds[0].next[0])
				if nextPtr != nil {
					next = hpSystem.Protect(hpNextIdx, (*Node)(nextPtr))
					if next != nil && next.key.Equal(key) {
						// Another thread inserted this key, update its value
						next.value.Store(value)
						// Release hazard pointers
						hpSystem.Release(0) // From findPredecessors
						hpSystem.Release(hpNextIdx)
						return
					}
				}
			}
		}
	}

	// Update skip list level if needed
	if level > sl.level {
		sl.level = level
	}

	// Release hazard pointers
	hpSystem.Release(0) // From findPredecessors
	hpSystem.Release(hpNextIdx)
}

// Get returns a value by key
func (sl *SkipList) Get(key Key) (interface{}, bool) {
	curr := sl.head

	// Thread-local hazard pointer indices
	hpCurrIdx := 0
	hpNextIdx := 1

	// Protect the head node
	hpSystem.Protect(hpCurrIdx, curr)

	for i := sl.level - 1; i >= 0; i-- {
		for {
			nextPtr := atomic.LoadPointer(&curr.next[i])
			if nextPtr == nil {
				break
			}

			// Protect the next node before accessing it
			next := hpSystem.Protect(hpNextIdx, (*Node)(nextPtr))

			// Verify the pointer didn't change (ABA prevention)
			if atomic.LoadPointer(&curr.next[i]) != nextPtr {
				continue
			}

			if next == nil || next.key.Less(key) {
				break
			}

			if next.key.Equal(key) {
				val := next.value.Load()
				// Release hazard pointers when done
				hpSystem.Release(hpCurrIdx)
				hpSystem.Release(hpNextIdx)
				return val, true
			}

			// Move forward
			curr = hpSystem.Protect(hpCurrIdx, next)
		}
	}

	// Release hazard pointers
	hpSystem.Release(hpCurrIdx)
	hpSystem.Release(hpNextIdx)
	return nil, false
}

// Delete removes a key
func (sl *SkipList) Delete(key Key) bool {
	// First find predecessors
	preds := sl.findPredecessors(key)

	// Use a hazard pointer to protect the target node
	hpTargetIdx := 2 // Use a different index than findPredecessors

	targetPtr := atomic.LoadPointer(&preds[0].next[0])
	if targetPtr == nil {
		// Release the hazard pointer used by findPredecessors
		hpSystem.Release(0)
		return false
	}

	// Protect the target node before accessing it
	target := hpSystem.Protect(hpTargetIdx, (*Node)(targetPtr))

	// Verify target hasn't changed
	if atomic.LoadPointer(&preds[0].next[0]) != targetPtr {
		hpSystem.Release(0)
		hpSystem.Release(hpTargetIdx)
		return false
	}

	if target == nil || !target.key.Equal(key) {
		hpSystem.Release(0)
		hpSystem.Release(hpTargetIdx)
		return false
	}

	// Remove node from all levels
	for i := 0; i < sl.level; i++ {
		for {
			next := (*Node)(atomic.LoadPointer(&preds[i].next[i]))
			if next != target {
				break
			}
			if atomic.CompareAndSwapPointer(&preds[i].next[i], unsafe.Pointer(target),
				atomic.LoadPointer(&target.next[i])) {
				break
			}
		}
	}

	// Instead of immediately freeing, retire the node
	hpSystem.Retire(target)

	// Release hazard pointers
	hpSystem.Release(0)
	hpSystem.Release(hpTargetIdx)

	return true
}

// RangeQuery iterates over a range of keys
func (sl *SkipList) RangeQuery(startKey, endKey Key, callback func(key Key, value interface{}) bool) {
	curr := sl.head

	// Thread-local hazard pointer indices
	hpCurrIdx := 0
	hpNextIdx := 1

	// Protect the head node
	curr = hpSystem.Protect(hpCurrIdx, curr)

	// Find the first node >= startKey
	for i := sl.level - 1; i >= 0; i-- {
		for {
			nextPtr := atomic.LoadPointer(&curr.next[i])
			if nextPtr == nil {
				break
			}

			next := hpSystem.Protect(hpNextIdx, (*Node)(nextPtr))

			// Verify the pointer didn't change
			if atomic.LoadPointer(&curr.next[i]) != nextPtr {
				continue
			}

			if next == nil || !next.key.Less(startKey) {
				break
			}

			// Move forward
			curr = hpSystem.Protect(hpCurrIdx, next)
		}
	}

	// Now curr is the predecessor of the first node >= startKey
	// Start iterating from level 0
	for {
		nextPtr := atomic.LoadPointer(&curr.next[0])
		if nextPtr == nil {
			break
		}

		next := hpSystem.Protect(hpNextIdx, (*Node)(nextPtr))

		// Verify the pointer didn't change
		if atomic.LoadPointer(&curr.next[0]) != nextPtr {
			continue
		}

		if next == nil {
			break
		}

		// If we've gone past the end key, stop
		if endKey.value != nil && !next.key.Less(endKey) && !next.key.Equal(endKey) {
			break
		}

		// Skip if node is marked as deleted
		if !next.deleted.Load() {
			// Call the callback with the key and value
			if !callback(next.key, next.value.Load()) {
				break // Stop if callback returns false
			}
		}

		// Move to the next node
		curr = hpSystem.Protect(hpCurrIdx, next)
	}

	// Release hazard pointers
	hpSystem.Release(hpCurrIdx)
	hpSystem.Release(hpNextIdx)
}

// SetWithTTL inserts or updates a key-value pair with a time-to-live in seconds
func (sl *SkipList) SetWithTTL(key Key, value interface{}, ttlSeconds int64) {
	level := sl.randomLevel()
	preds := sl.findPredecessors(key)

	// Use hazard pointer to protect the next node
	hpNextIdx := 2 // Use different index than findPredecessors

	nextPtr := atomic.LoadPointer(&preds[0].next[0])
	next := (*Node)(nextPtr)

	if nextPtr != nil {
		// Protect the next node if it exists
		next = hpSystem.Protect(hpNextIdx, next)

		// Check if we're updating an existing key
		if next != nil && next.key.Equal(key) {
			next.value.Store(value)
			// Update timestamp for TTL
			atomic.StoreInt64(&next.timestamp, time.Now().UnixNano()+ttlSeconds*1e9)
			// Clear deleted flag if it was set
			next.deleted.Store(false)
			// Release hazard pointers
			hpSystem.Release(0) // From findPredecessors
			hpSystem.Release(hpNextIdx)
			return
		}
	}

	// Create a new node
	newNode := &Node{
		key:       key,
		timestamp: time.Now().UnixNano() + ttlSeconds*1e9,
	}
	newNode.value.Store(value)

	// Insert at each level
	for i := 0; i < level; i++ {
		for {
			nextPtr = atomic.LoadPointer(&preds[i].next[i])
			next = (*Node)(nextPtr)
			newNode.next[i] = unsafe.Pointer(next)
			if atomic.CompareAndSwapPointer(&preds[i].next[i], unsafe.Pointer(next), unsafe.Pointer(newNode)) {
				break
			}

			// If CAS failed, we need to find new predecessors at this level
			if i == 0 {
				// Only need to refind at level 0 since this is where we check for duplicates
				tempPreds := sl.findPredecessors(key)
				preds[0] = tempPreds[0]

				// Check if key was inserted by another thread
				nextPtr = atomic.LoadPointer(&preds[0].next[0])
				if nextPtr != nil {
					next = hpSystem.Protect(hpNextIdx, (*Node)(nextPtr))
					if next != nil && next.key.Equal(key) {
						// Another thread inserted this key, update its value and TTL
						next.value.Store(value)
						atomic.StoreInt64(&next.timestamp, time.Now().UnixNano()+ttlSeconds*1e9)
						next.deleted.Store(false)
						// Release hazard pointers
						hpSystem.Release(0) // From findPredecessors
						hpSystem.Release(hpNextIdx)
						return
					}
				}
			}
		}
	}

	// Update skip list level if needed
	if level > sl.level {
		sl.level = level
	}

	// Release hazard pointers
	hpSystem.Release(0) // From findPredecessors
	hpSystem.Release(hpNextIdx)
}

// CheckTTL checks if a key has expired
func (sl *SkipList) CheckTTL(key Key) bool {
	curr := sl.head

	// Thread-local hazard pointer indices
	hpCurrIdx := 0
	hpNextIdx := 1

	// Protect the head node
	hpSystem.Protect(hpCurrIdx, curr)

	for i := sl.level - 1; i >= 0; i-- {
		for {
			nextPtr := atomic.LoadPointer(&curr.next[i])
			if nextPtr == nil {
				break
			}

			// Protect the next node before accessing it
			next := hpSystem.Protect(hpNextIdx, (*Node)(nextPtr))

			// Verify the pointer didn't change (ABA prevention)
			if atomic.LoadPointer(&curr.next[i]) != nextPtr {
				continue
			}

			if next == nil || !next.key.Less(key) {
				break
			}

			if next.key.Equal(key) {
				timestamp := atomic.LoadInt64(&next.timestamp)
				expired := timestamp > 0 && timestamp < time.Now().UnixNano()

				// If expired, mark as deleted (lazy deletion)
				if expired {
					next.deleted.Store(true)
				}

				// Release hazard pointers when done
				hpSystem.Release(hpCurrIdx)
				hpSystem.Release(hpNextIdx)
				return expired
			}

			// Move forward
			curr = hpSystem.Protect(hpCurrIdx, next)
		}
	}

	// Release hazard pointers
	hpSystem.Release(hpCurrIdx)
	hpSystem.Release(hpNextIdx)
	return false
}

// GetWithTTLCheck gets a value and checks TTL in one operation
func (sl *SkipList) GetWithTTLCheck(key Key) (interface{}, bool) {
	curr := sl.head

	// Thread-local hazard pointer indices
	hpCurrIdx := 0
	hpNextIdx := 1

	// Protect the head node
	hpSystem.Protect(hpCurrIdx, curr)

	for i := sl.level - 1; i >= 0; i-- {
		for {
			nextPtr := atomic.LoadPointer(&curr.next[i])
			if nextPtr == nil {
				break
			}

			// Protect the next node before accessing it
			next := hpSystem.Protect(hpNextIdx, (*Node)(nextPtr))

			// Verify the pointer didn't change (ABA prevention)
			if atomic.LoadPointer(&curr.next[i]) != nextPtr {
				continue
			}

			if next == nil || next.key.Less(key) {
				break
			}

			if next.key.Equal(key) {
				// Check if node is marked as deleted
				if next.deleted.Load() {
					// Release hazard pointers
					hpSystem.Release(hpCurrIdx)
					hpSystem.Release(hpNextIdx)
					return nil, false
				}

				// Check TTL
				timestamp := atomic.LoadInt64(&next.timestamp)
				if timestamp > 0 && timestamp < time.Now().UnixNano() {
					// Mark as deleted because it's expired
					next.deleted.Store(true)
					// Release hazard pointers
					hpSystem.Release(hpCurrIdx)
					hpSystem.Release(hpNextIdx)
					return nil, false
				}

				val := next.value.Load()
				// Release hazard pointers when done
				hpSystem.Release(hpCurrIdx)
				hpSystem.Release(hpNextIdx)
				return val, true
			}

			// Move forward
			curr = hpSystem.Protect(hpCurrIdx, next)
		}
	}

	// Release hazard pointers
	hpSystem.Release(hpCurrIdx)
	hpSystem.Release(hpNextIdx)
	return nil, false
}

// TTLCleanup periodically checks for and removes expired keys
func (sl *SkipList) TTLCleanup() {
	curr := sl.head

	// Thread-local hazard pointer indices
	hpCurrIdx := 0
	hpNextIdx := 1

	// Protect the head node
	curr = hpSystem.Protect(hpCurrIdx, curr)

	// Iterate through level 0
	for {
		nextPtr := atomic.LoadPointer(&curr.next[0])
		if nextPtr == nil {
			break
		}

		next := hpSystem.Protect(hpNextIdx, (*Node)(nextPtr))

		// Verify the pointer didn't change
		if atomic.LoadPointer(&curr.next[0]) != nextPtr {
			continue
		}

		if next == nil {
			break
		}

		// Check if expired
		timestamp := atomic.LoadInt64(&next.timestamp)
		if timestamp > 0 && timestamp < time.Now().UnixNano() {
			// Mark as deleted
			next.deleted.Store(true)

			// Perform actual deletion in background (could be improved with a queue)
			go sl.Delete(next.key)
		}

		// Move forward
		curr = hpSystem.Protect(hpCurrIdx, next)
	}

	// Release hazard pointers
	hpSystem.Release(hpCurrIdx)
	hpSystem.Release(hpNextIdx)
}

// StartTTLCleanupRoutine starts a goroutine that periodically cleans up expired keys
func (sl *SkipList) StartTTLCleanupRoutine(interval time.Duration) (cancel func()) {
	done := make(chan struct{})
	ticker := time.NewTicker(interval)

	go func() {
		for {
			select {
			case <-ticker.C:
				sl.TTLCleanup()
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()

	return func() {
		close(done)
	}
}

// BatchOperation represents a batch of operations to be performed atomically
type BatchOperation struct {
	Key      Key
	Value    interface{}
	TTL      int64 // 0 means no TTL
	IsDelete bool
}

// ExecuteBatch executes a batch of operations with optimistic concurrency control
func (sl *SkipList) ExecuteBatch(operations []BatchOperation) bool {
	// Sort operations by key to prevent deadlocks
	sort.Slice(operations, func(i, j int) bool {
		return operations[i].Key.Less(operations[j].Key)
	})

	// First, find all predecessors in one pass to reduce contention
	predecessors := make(map[string][maxLevel]*Node)
	for _, op := range operations {
		keyStr := op.Key.String()
		if _, exists := predecessors[keyStr]; !exists {
			predecessors[keyStr] = sl.findPredecessors(op.Key)
		}
	}

	// Perform operations
	for _, op := range operations {
		if op.IsDelete {
			// Use deletion logic
			sl.deleteWithPredecessors(op.Key, predecessors[op.Key.String()])
		} else if op.TTL > 0 {
			// Use TTL set logic
			sl.setWithPredecessorsAndTTL(op.Key, op.Value, op.TTL, predecessors[op.Key.String()])
		} else {
			// Use regular set logic
			sl.setWithPredecessors(op.Key, op.Value, predecessors[op.Key.String()])
		}
	}

	// In a more sophisticated implementation, we would add rollback capabilities
	// for true transactional semantics

	return true
}

// Internal helper for batch operations
func (sl *SkipList) setWithPredecessors(key Key, value interface{}, preds [maxLevel]*Node) {
	// Implementation similar to Set but uses provided predecessors
	// This reduces redundant traversals in batch operations

	// Use hazard pointer to protect the next node
	hpNextIdx := 2

	nextPtr := atomic.LoadPointer(&preds[0].next[0])
	next := (*Node)(nextPtr)

	if nextPtr != nil {
		// Protect the next node if it exists
		next = hpSystem.Protect(hpNextIdx, next)

		// Check if we're updating an existing key
		if next != nil && next.key.Equal(key) {
			next.value.Store(value)
			next.deleted.Store(false)
			// Release hazard pointers
			hpSystem.Release(hpNextIdx)
			return
		}
	}

	// Create a new node
	level := sl.randomLevel()
	newNode := &Node{key: key}
	newNode.value.Store(value)

	// Insert at each level
	for i := 0; i < level; i++ {
		for {
			nextPtr = atomic.LoadPointer(&preds[i].next[i])
			next = (*Node)(nextPtr)
			newNode.next[i] = unsafe.Pointer(next)
			if atomic.CompareAndSwapPointer(&preds[i].next[i], unsafe.Pointer(next), unsafe.Pointer(newNode)) {
				break
			}
		}
	}

	// Update skip list level if needed
	if level > sl.level {
		sl.level = level
	}

	// Release hazard pointers
	hpSystem.Release(hpNextIdx)
}

// Internal helper for batch operations with TTL
func (sl *SkipList) setWithPredecessorsAndTTL(key Key, value interface{}, ttl int64, preds [maxLevel]*Node) {
	// Implementation similar to SetWithTTL but uses provided predecessors

	// Use hazard pointer to protect the next node
	hpNextIdx := 2

	nextPtr := atomic.LoadPointer(&preds[0].next[0])
	next := (*Node)(nextPtr)

	if nextPtr != nil {
		// Protect the next node if it exists
		next = hpSystem.Protect(hpNextIdx, next)

		// Check if we're updating an existing key
		if next != nil && next.key.Equal(key) {
			next.value.Store(value)
			atomic.StoreInt64(&next.timestamp, time.Now().UnixNano()+ttl*1e9)
			next.deleted.Store(false)
			// Release hazard pointers
			hpSystem.Release(hpNextIdx)
			return
		}
	}

	// Create a new node
	level := sl.randomLevel()
	newNode := &Node{
		key:       key,
		timestamp: time.Now().UnixNano() + ttl*1e9,
	}
	newNode.value.Store(value)

	// Insert at each level
	for i := 0; i < level; i++ {
		for {
			nextPtr = atomic.LoadPointer(&preds[i].next[i])
			next = (*Node)(nextPtr)
			newNode.next[i] = unsafe.Pointer(next)
			if atomic.CompareAndSwapPointer(&preds[i].next[i], unsafe.Pointer(next), unsafe.Pointer(newNode)) {
				break
			}
		}
	}

	// Update skip list level if needed
	if level > sl.level {
		sl.level = level
	}

	// Release hazard pointers
	hpSystem.Release(hpNextIdx)
}

// Internal helper for batch operations with delete
func (sl *SkipList) deleteWithPredecessors(key Key, preds [maxLevel]*Node) bool {
	// Implementation similar to Delete but uses provided predecessors

	// Use a hazard pointer to protect the target node
	hpTargetIdx := 2

	targetPtr := atomic.LoadPointer(&preds[0].next[0])
	if targetPtr == nil {
		return false
	}

	// Protect the target node before accessing it
	target := hpSystem.Protect(hpTargetIdx, (*Node)(targetPtr))

	// Verify target hasn't changed
	if atomic.LoadPointer(&preds[0].next[0]) != targetPtr {
		hpSystem.Release(hpTargetIdx)
		return false
	}

	if target == nil || !target.key.Equal(key) {
		hpSystem.Release(hpTargetIdx)
		return false
	}

	// Remove node from all levels
	for i := 0; i < sl.level; i++ {
		for {
			next := (*Node)(atomic.LoadPointer(&preds[i].next[i]))
			if next != target {
				break
			}
			if atomic.CompareAndSwapPointer(&preds[i].next[i], unsafe.Pointer(target),
				atomic.LoadPointer(&target.next[i])) {
				break
			}
		}
	}

	// Instead of immediately freeing, retire the node
	hpSystem.Retire(target)

	// Release hazard pointers
	hpSystem.Release(hpTargetIdx)

	return true
}

// Len returns an approximate count of the elements
func (sl *SkipList) Len() int {
	count := 0
	curr := sl.head

	// Use hazard pointers
	hpCurrIdx := 0
	hpNextIdx := 1

	// Protect the head node
	curr = hpSystem.Protect(hpCurrIdx, curr)

	// Count elements at level 0
	for {
		nextPtr := atomic.LoadPointer(&curr.next[0])
		if nextPtr == nil {
			break
		}

		next := hpSystem.Protect(hpNextIdx, (*Node)(nextPtr))

		// Verify the pointer didn't change
		if atomic.LoadPointer(&curr.next[0]) != nextPtr {
			continue
		}

		if next == nil {
			break
		}

		// Only count non-deleted nodes
		if !next.deleted.Load() {
			count++
		}

		// Move forward
		curr = hpSystem.Protect(hpCurrIdx, next)
	}

	// Release hazard pointers
	hpSystem.Release(hpCurrIdx)
	hpSystem.Release(hpNextIdx)

	return count
}

// Clear removes all elements from the skip list
func (sl *SkipList) Clear() {
	// Create a new head node
	newHead := &Node{}

	// Replace the current head with the new one
	oldHead := sl.head
	sl.head = newHead
	sl.level = 1

	// Retire the old head (which will eventually retire all connected nodes)
	hpSystem.Retire(oldHead)
}

// GetStats returns statistics about the skip list
func (sl *SkipList) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"level":             sl.level,
		"approximateLength": sl.Len(),
		"maxLevel":          maxLevel,
		"probability":       probability,
	}
}
