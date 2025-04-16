package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"db-mem-golang/database"
	"db-mem-golang/skiplist"
)

// Person represents a sample data structure for our database
type Person struct {
	ID        int
	Name      string
	Email     string
	Age       int
	CreatedAt time.Time
}

func main() {
	fmt.Println("=== Fast In-Memory Database with Lock-Free Skip Lists ===")

	// Create a new database with automatic TTL cleanup
	db := database.NewDatabase(
		database.WithTTLCleanup(true),
		database.WithGCInterval(1*time.Minute),
	)
	defer db.Close()

	// Create a collection for storing people
	people, err := db.CreateCollection("people")
	if err != nil {
		fmt.Printf("Error creating collection: %v\n", err)
		return
	}

	// Create indexes for efficient querying
	nameIndex, err := people.CreateIndex("name_idx", func(val interface{}) (skiplist.Key, error) {
		if person, ok := val.(Person); ok {
			return skiplist.NewKey(person.Name)
		}
		return skiplist.Key{}, fmt.Errorf("invalid type for name index")
	})
	if err != nil {
		fmt.Printf("Error creating name index: %v\n", err)
	}

	ageIndex, err := people.CreateIndex("age_idx", func(val interface{}) (skiplist.Key, error) {
		if person, ok := val.(Person); ok {
			return skiplist.NewKey(person.Age)
		}
		return skiplist.Key{}, fmt.Errorf("invalid type for age index")
	})
	if err != nil {
		fmt.Printf("Error creating age index: %v\n", err)
	}

	// Add some sample data
	addSamplePeople(people)

	// Retrieve and display a person
	val, found := people.Get(42)
	if found {
		person := val.(Person)
		fmt.Printf("Found person: ID=%d, Name=%s, Age=%d\n",
			person.ID, person.Name, person.Age)
	}

	// Perform a query using an index
	fmt.Println("\nQuerying by name 'Alice':")
	results, err := nameIndex.Query("Alice")
	if err == nil && len(results) > 0 {
		for _, result := range results {
			person := result.(Person)
			fmt.Printf("  Found: ID=%d, Name=%s, Age=%d\n",
				person.ID, person.Name, person.Age)
		}
	}

	// Perform a range query using the age index
	fmt.Println("\nQuerying people between ages 25 and 35:")
	ageResults, err := ageIndex.RangeQuery(25, 35)
	if err == nil {
		for _, result := range ageResults {
			person := result.(Person)
			fmt.Printf("  Found: ID=%d, Name=%s, Age=%d\n",
				person.ID, person.Name, person.Age)
		}
	}

	// Demonstrate TTL (expiring data)
	fmt.Println("\nDemonstrating TTL (Time-To-Live):")
	people.SetWithTTL(999, Person{
		ID:   999,
		Name: "Temporary",
		Age:  100,
	}, 5) // 5 seconds TTL

	fmt.Println("Added temporary entry with ID=999 and 5 second TTL")

	// Check immediately
	if val, found := people.Get(999); found {
		person := val.(Person)
		fmt.Printf("  Found temporary entry: %s\n", person.Name)
	}

	// Demonstrate transactions
	fmt.Println("\nDemonstrating transactions:")
	tx := people.NewTransaction()

	// Add multiple operations to the transaction
	tx.Set(1001, Person{ID: 1001, Name: "Transaction1", Age: 31})
	tx.Set(1002, Person{ID: 1002, Name: "Transaction2", Age: 32})
	tx.SetWithTTL(1003, Person{ID: 1003, Name: "TransactionWithTTL", Age: 33}, 3600)

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		fmt.Printf("  Transaction failed: %v\n", err)
	} else {
		fmt.Printf("  Transaction committed with %d operations\n", tx.Size())
	}

	// Verify the transaction results
	if val, found := people.Get(1001); found {
		person := val.(Person)
		fmt.Printf("  Transaction entry: %s\n", person.Name)
	}

	// Run a multi-threaded benchmark
	fmt.Println("\nRunning concurrent performance test...")
	runConcurrentBenchmark(people)

	// Display database stats
	fmt.Println("\nDatabase statistics:")
	stats := db.Stats()
	fmt.Printf("  Collections: %d\n", stats["collections"])

	collStats := stats["collectionStats"].(map[string]interface{})
	peopleStats := collStats["people"].(map[string]interface{})
	fmt.Printf("  People collection:\n")
	fmt.Printf("    Count: %v\n", peopleStats["count"])
	fmt.Printf("    Reads: %v\n", peopleStats["reads"])
	fmt.Printf("    Writes: %v\n", peopleStats["writes"])
	fmt.Printf("    Indexes: %v\n", peopleStats["indexes"])

	// Wait to check if TTL entry has expired
	fmt.Println("\nWaiting 6 seconds to verify TTL expiration...")
	time.Sleep(6 * time.Second)

	if _, found := people.Get(999); found {
		fmt.Println("  TTL entry still exists (unexpected)")
	} else {
		fmt.Println("  TTL entry has expired as expected")
	}
}

// addSamplePeople adds some test data to the collection
func addSamplePeople(people *database.Collection) {
	samplePeople := []Person{
		{ID: 42, Name: "Alice", Email: "alice@example.com", Age: 28, CreatedAt: time.Now()},
		{ID: 43, Name: "Bob", Email: "bob@example.com", Age: 32, CreatedAt: time.Now()},
		{ID: 44, Name: "Charlie", Email: "charlie@example.com", Age: 24, CreatedAt: time.Now()},
		{ID: 45, Name: "Diana", Email: "diana@example.com", Age: 35, CreatedAt: time.Now()},
		{ID: 46, Name: "Evan", Email: "evan@example.com", Age: 29, CreatedAt: time.Now()},
	}

	for _, person := range samplePeople {
		people.Set(person.ID, person)
	}

	fmt.Printf("Added %d sample people to the database\n", len(samplePeople))
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
