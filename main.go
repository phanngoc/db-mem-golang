package main

import (
	"db-mem-golang/database"
	"db-mem-golang/skiplist"
	"db-mem-golang/wal"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Person represents a sample data structure for our database
type Person struct {
	ID        int       `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	Age       int       `json:"age"`
	CreatedAt time.Time `json:"created_at"`
}

// APIResponse is a generic response structure
type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

// CollectionInfo contains metadata about a collection
type CollectionInfo struct {
	Name    string   `json:"name"`
	Count   int      `json:"count"`
	Indexes []string `json:"indexes"`
}

// Global database instance for API access
var dbInstance *database.Database
var collections = make(map[string]*database.Collection)
var indexes = make(map[string]map[string]*database.Index)

func init() {
	// Register types for gob encoding/decoding
	gob.Register(Person{})
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
}

func main() {
	fmt.Println("=== Fast In-Memory Database with Lock-Free Skip Lists ===")

	// Configure WAL options
	walOptions := wal.Options{
		Dir:                "data",
		SyncInterval:       200 * time.Millisecond,
		CheckpointInterval: 5 * time.Minute,
		MaxLogSize:         50 * 1024 * 1024, // 50MB
	}

	// Create a new database with WAL enabled
	dbInstance = database.NewDatabase(
		database.WithTTLCleanup(true),
		database.WithGCInterval(1*time.Minute),
		database.WithWAL(walOptions),
		database.WithRecovery(true),
	)

	// Set up improved HTTP API
	setupHTTPAPI()
	// Set up clean shutdown
	setupGracefulShutdown()

	// Wait forever (server runs in a goroutine)
	select {}
}

// setupGracefulShutdown ensures the database is properly closed on shutdown
func setupGracefulShutdown() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		fmt.Println("\nShutting down database...")
		dbInstance.Close()
		os.Exit(0)
	}()
}

// setupHTTPAPI initializes the HTTP server with improved API routes
func setupHTTPAPI() {
	// API version prefix
	const apiPrefix = "/api/v1"

	// Root endpoint with API information
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			Message: "In-Memory Database API",
			Data: map[string]interface{}{
				"version":              "1.0.0",
				"documentation":        apiPrefix + "/docs",
				"collections_endpoint": apiPrefix + "/collections",
			},
		})
	})

	// API Documentation
	http.HandleFunc(apiPrefix+"/docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			Data: map[string]interface{}{
				"endpoints": []map[string]string{
					{"method": "GET", "path": apiPrefix + "/collections", "description": "List all collections"},
					{"method": "POST", "path": apiPrefix + "/collections", "description": "Create a new collection"},
					{"method": "GET", "path": apiPrefix + "/collections/{name}", "description": "Get collection info"},
					{"method": "DELETE", "path": apiPrefix + "/collections/{name}", "description": "Delete a collection"},
					{"method": "GET", "path": apiPrefix + "/collections/{name}/items", "description": "List items in a collection"},
					{"method": "POST", "path": apiPrefix + "/collections/{name}/items", "description": "Add an item to a collection"},
					{"method": "GET", "path": apiPrefix + "/collections/{name}/items/{id}", "description": "Get an item by ID"},
					{"method": "PUT", "path": apiPrefix + "/collections/{name}/items/{id}", "description": "Update an item"},
					{"method": "DELETE", "path": apiPrefix + "/collections/{name}/items/{id}", "description": "Delete an item"},
					{"method": "POST", "path": apiPrefix + "/collections/{name}/indexes", "description": "Create an index"},
					{"method": "GET", "path": apiPrefix + "/collections/{name}/indexes", "description": "List indexes"},
					{"method": "GET", "path": apiPrefix + "/collections/{name}/search", "description": "Search in a collection"},
					{"method": "GET", "path": apiPrefix + "/stats", "description": "Get database statistics"},
				},
			},
		})
	})

	// Collections management endpoints
	http.HandleFunc(apiPrefix+"/collections", handleCollections)
	http.HandleFunc(apiPrefix+"/collections/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, apiPrefix+"/collections/")
		segments := strings.Split(path, "/")

		if len(segments) == 1 && segments[0] != "" {
			// Handle collection-level operations
			handleCollection(w, r, segments[0])
			return
		} else if len(segments) >= 2 && segments[1] == "items" {
			if len(segments) == 2 {
				// Handle items list/create
				handleCollectionItems(w, r, segments[0])
				return
			} else if len(segments) == 3 && segments[2] != "" {
				// Handle specific item operations
				handleCollectionItem(w, r, segments[0], segments[2])
				return
			}
		} else if len(segments) >= 2 && segments[1] == "indexes" {
			// Handle collection indexes
			handleCollectionIndexes(w, r, segments[0])
			return
		} else if len(segments) >= 2 && segments[1] == "search" {
			// Handle collection search
			handleCollectionSearch(w, r, segments[0])
			return
		}

		http.NotFound(w, r)
	})

	// Database stats
	http.HandleFunc(apiPrefix+"/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		stats := dbInstance.Stats()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			Data:    stats,
		})
	})

	// Start HTTP server
	port := "8080"
	fmt.Printf("\nStarting HTTP server on port %s...\n", port)
	fmt.Printf("API available at http://localhost:%s%s\n", port, apiPrefix)

	go func() {
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()
}

// handleCollections handles listing all collections and creating new ones
func handleCollections(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		// List all collections
		stats := dbInstance.Stats()
		collStats := stats["collectionStats"].(map[string]interface{})

		collectionList := make([]CollectionInfo, 0)
		for name, statsData := range collStats {
			collData := statsData.(map[string]interface{})

			// Get indexes for this collection
			var indexNames []string
			if idxMap, exists := indexes[name]; exists {
				for idxName := range idxMap {
					indexNames = append(indexNames, idxName)
				}
			}

			collectionList = append(collectionList, CollectionInfo{
				Name:    name,
				Count:   collData["count"].(int),
				Indexes: indexNames,
			})
		}

		json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			Data:    collectionList,
		})

	case http.MethodPost:
		// Create a new collection
		var reqData struct {
			Name string `json:"name"`
		}

		if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil {
			sendErrorResponse(w, "Invalid request", http.StatusBadRequest)
			return
		}

		if reqData.Name == "" {
			sendErrorResponse(w, "Collection name is required", http.StatusBadRequest)
			return
		}

		collection, err := dbInstance.CreateCollection(reqData.Name)
		if err != nil {
			sendErrorResponse(w, fmt.Sprintf("Failed to create collection: %v", err), http.StatusInternalServerError)
			return
		}

		collections[reqData.Name] = collection
		indexes[reqData.Name] = make(map[string]*database.Index)

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			Message: "Collection created successfully",
			Data: CollectionInfo{
				Name:    reqData.Name,
				Count:   0,
				Indexes: []string{},
			},
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleCollection handles operations on a specific collection
func handleCollection(w http.ResponseWriter, r *http.Request, collectionName string) {
	w.Header().Set("Content-Type", "application/json")

	// Check if collection exists
	_, exists := collections[collectionName]
	if !exists {
		sendErrorResponse(w, "Collection not found", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodGet:
		// Get collection info
		stats := dbInstance.Stats()
		collStats := stats["collectionStats"].(map[string]interface{})
		collData := collStats[collectionName].(map[string]interface{})

		// Get indexes for this collection
		var indexNames []string
		if idxMap, exists := indexes[collectionName]; exists {
			for idxName := range idxMap {
				indexNames = append(indexNames, idxName)
			}
		}

		json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			Data: map[string]interface{}{
				"name":    collectionName,
				"stats":   collData,
				"indexes": indexNames,
			},
		})

	case http.MethodDelete:
		// Delete collection (not directly supported in the current DB API)
		// We'll just remove it from our tracking maps
		delete(collections, collectionName)
		delete(indexes, collectionName)

		json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			Message: "Collection removed",
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleCollectionItems handles listing all items in a collection and adding new items
func handleCollectionItems(w http.ResponseWriter, r *http.Request, collectionName string) {
	w.Header().Set("Content-Type", "application/json")

	// Check if collection exists
	collection, exists := collections[collectionName]
	if !exists {
		sendErrorResponse(w, "Collection not found", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodGet:
		// List all items (with pagination)
		limit := 100 // Default limit
		offset := 0  // Default offset

		// Parse pagination parameters
		if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
			if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
				limit = parsedLimit
			}
		}

		if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
			if parsedOffset, err := strconv.Atoi(offsetStr); err == nil && parsedOffset >= 0 {
				offset = parsedOffset
			}
		}

		// This is not efficient for large collections but works for this demo
		// A real implementation would need a way to iterate over collection items
		items := make([]interface{}, 0)
		count := 0
		skipped := 0

		// Simple approach: scan through a reasonable ID range
		// This assumes IDs are integers and reasonably dense
		maxID := 100000 // Arbitrary limit for scanning

		for i := 0; i < maxID; i++ {
			if val, found := collection.Get(i); found {
				if skipped < offset {
					skipped++
					continue
				}

				items = append(items, val)
				count++

				if count >= limit {
					break
				}
			}
		}

		json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			Data: map[string]interface{}{
				"items":  items,
				"limit":  limit,
				"offset": offset,
				"count":  count,
			},
		})

	case http.MethodPost:
		// Add a new item
		var item interface{}
		if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
			sendErrorResponse(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// For now, we'll assume the item has an ID field and extract it
		// This is a simplified approach
		var itemID interface{}

		if mapItem, ok := item.(map[string]interface{}); ok {
			if id, exists := mapItem["id"]; exists {
				itemID = id
			} else {
				// Generate a random ID if none provided
				newID := rand.Intn(10000) + 1000
				mapItem["id"] = newID
				itemID = newID
			}
		} else {
			sendErrorResponse(w, "Item must be a JSON object with an ID field", http.StatusBadRequest)
			return
		}

		// Store the item
		collection.Set(itemID, item)

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			Message: "Item added to collection",
			Data:    item,
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleCollectionItem handles operations on a specific item in a collection
func handleCollectionItem(w http.ResponseWriter, r *http.Request, collectionName, itemIDStr string) {
	w.Header().Set("Content-Type", "application/json")

	// Check if collection exists
	collection, exists := collections[collectionName]
	if !exists {
		sendErrorResponse(w, "Collection not found", http.StatusNotFound)
		return
	}

	// Convert ID to appropriate type for database lookup
	var itemID interface{}
	if intID, err := strconv.Atoi(itemIDStr); err == nil {
		// If it's an integer, use the int value
		itemID = intID
	} else {
		// If not an integer, use the string as is (allows string keys too)
		itemID = itemIDStr
	}

	switch r.Method {
	case http.MethodGet:
		// Get an item by ID
		if val, found := collection.Get(itemID); found {
			json.NewEncoder(w).Encode(APIResponse{
				Success: true,
				Data:    val,
			})
		} else {
			sendErrorResponse(w, "Item not found", http.StatusNotFound)
		}

	case http.MethodPut:
		// Update an item
		var updatedItem interface{}
		if err := json.NewDecoder(r.Body).Decode(&updatedItem); err != nil {
			sendErrorResponse(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Check if item exists
		if _, found := collection.Get(itemID); !found {
			sendErrorResponse(w, "Item not found", http.StatusNotFound)
			return
		}

		// Update the item
		collection.Set(itemID, updatedItem)

		json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			Message: "Item updated",
			Data:    updatedItem,
		})

	case http.MethodDelete:
		// Delete an item
		if _, found := collection.Get(itemID); !found {
			sendErrorResponse(w, "Item not found", http.StatusNotFound)
			return
		}

		collection.Delete(itemID)

		json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			Message: "Item deleted",
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleCollectionIndexes handles creating and listing indexes for a collection
func handleCollectionIndexes(w http.ResponseWriter, r *http.Request, collectionName string) {
	w.Header().Set("Content-Type", "application/json")

	// Check if collection exists
	collection, exists := collections[collectionName]
	if !exists {
		sendErrorResponse(w, "Collection not found", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodGet:
		// List indexes
		collIndexes, exists := indexes[collectionName]
		if !exists {
			collIndexes = make(map[string]*database.Index)
			indexes[collectionName] = collIndexes
		}

		indexList := make([]string, 0, len(collIndexes))
		for name := range collIndexes {
			indexList = append(indexList, name)
		}

		json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			Data:    indexList,
		})

	case http.MethodPost:
		// Create a new index
		var reqData struct {
			Name      string `json:"name"`
			Field     string `json:"field"`
			IndexType string `json:"type"` // "string", "int", etc.
		}

		if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil {
			sendErrorResponse(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		if reqData.Name == "" || reqData.Field == "" {
			sendErrorResponse(w, "Index name and field are required", http.StatusBadRequest)
			return
		}

		// Create the index based on field and type
		var indexFn func(interface{}) (skiplist.Key, error)

		switch reqData.IndexType {
		case "string":
			indexFn = func(val interface{}) (skiplist.Key, error) {
				if item, ok := val.(map[string]interface{}); ok {
					if fieldVal, exists := item[reqData.Field]; exists {
						if strVal, ok := fieldVal.(string); ok {
							return skiplist.NewKey(strVal)
						}
					}
				}
				return skiplist.Key{}, fmt.Errorf("invalid item or field for string index")
			}
		case "int":
			indexFn = func(val interface{}) (skiplist.Key, error) {
				if item, ok := val.(map[string]interface{}); ok {
					if fieldVal, exists := item[reqData.Field]; exists {
						if numVal, ok := fieldVal.(float64); ok {
							return skiplist.NewKey(int(numVal))
						}
					}
				}
				return skiplist.Key{}, fmt.Errorf("invalid item or field for int index")
			}
		default:
			// Default to string index
			indexFn = func(val interface{}) (skiplist.Key, error) {
				if item, ok := val.(map[string]interface{}); ok {
					if fieldVal, exists := item[reqData.Field]; exists {
						return skiplist.NewKey(fieldVal)
					}
				}
				return skiplist.Key{}, fmt.Errorf("invalid item or field")
			}
		}

		// Create the index
		idx, err := collection.CreateIndex(reqData.Name+"_idx", indexFn)
		if err != nil {
			sendErrorResponse(w, fmt.Sprintf("Failed to create index: %v", err), http.StatusInternalServerError)
			return
		}

		// Store the index in our tracking map
		if _, exists := indexes[collectionName]; !exists {
			indexes[collectionName] = make(map[string]*database.Index)
		}
		indexes[collectionName][reqData.Name] = idx

		json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			Message: "Index created successfully",
			Data: map[string]string{
				"name":  reqData.Name,
				"field": reqData.Field,
				"type":  reqData.IndexType,
			},
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleCollectionSearch handles searching within a collection using an index
func handleCollectionSearch(w http.ResponseWriter, r *http.Request, collectionName string) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Check if collection exists
	_, exists := collections[collectionName]
	if !exists {
		sendErrorResponse(w, "Collection not found", http.StatusNotFound)
		return
	}

	// Get search parameters
	indexName := r.URL.Query().Get("index")
	if indexName == "" {
		sendErrorResponse(w, "Index name is required", http.StatusBadRequest)
		return
	}

	// Check if the index exists
	collIndexes, exists := indexes[collectionName]
	if !exists || collIndexes == nil {
		sendErrorResponse(w, "No indexes found for this collection", http.StatusNotFound)
		return
	}

	index, exists := collIndexes[indexName]
	if !exists || index == nil {
		sendErrorResponse(w, "Index not found", http.StatusNotFound)
		return
	}

	// Determine search type: exact match or range query
	queryType := r.URL.Query().Get("type")
	var results []interface{}
	var err error

	if queryType == "range" {
		// Range query
		minStr := r.URL.Query().Get("min")
		maxStr := r.URL.Query().Get("max")

		if minStr == "" || maxStr == "" {
			sendErrorResponse(w, "Both min and max parameters are required for range queries", http.StatusBadRequest)
			return
		}

		// Try to parse as integers first
		min, err1 := strconv.Atoi(minStr)
		max, err2 := strconv.Atoi(maxStr)

		if err1 == nil && err2 == nil {
			// Integer range query
			results, err = index.RangeQuery(min, max)
		} else {
			// String range query
			results, err = index.RangeQuery(minStr, maxStr)
		}

	} else {
		// Exact match query
		value := r.URL.Query().Get("value")
		if value == "" {
			sendErrorResponse(w, "Search value is required", http.StatusBadRequest)
			return
		}

		// Try to parse as integer first
		if intVal, err := strconv.Atoi(value); err == nil {
			results, err = index.Query(intVal)
		} else {
			// String query
			results, err = index.Query(value)
		}
	}

	if err != nil {
		sendErrorResponse(w, fmt.Sprintf("Search error: %v", err), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"results": results,
			"count":   len(results),
		},
	})
}

// sendErrorResponse sends a standardized error response
func sendErrorResponse(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(APIResponse{
		Success: false,
		Message: message,
	})
}

// runConcurrentBenchmark tests the performance under high concurrency
func runConcurrentBenchmark(col *database.Collection) {
	const (
		numReaders = 8
		numWriters = 4
		numOps     = 10000
	)

	var wg sync.WaitGroup
	startTime := time.Now()

	// Start reader goroutines
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			for i := 0; i < numOps; i++ {
				key := rnd.Intn(numOps) + 1
				col.Get(key)
			}
		}(r)
	}

	// Start writer goroutines
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id+100)))
			for i := 0; i < numOps; i++ {
				key := rnd.Intn(numOps) + 1
				age := rnd.Intn(80) + 18
				col.Set(key, Person{
					ID:   key,
					Name: fmt.Sprintf("Person-%d", key),
					Age:  age,
				})
			}
		}(w)
	}

	// Wait for all operations to complete
	wg.Wait()
	duration := time.Since(startTime)

	totalOps := numOps * (numReaders + numWriters)
	opsPerSec := float64(totalOps) / duration.Seconds()

	fmt.Printf("  Completed %d operations in %v\n", totalOps, duration)
	fmt.Printf("  Performance: %.0f operations/second\n", opsPerSec)
}
