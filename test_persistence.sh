#!/bin/bash

# test_persistence.sh - Script to demonstrate WAL persistence

echo "Creating data directory if it doesn't exist..."
mkdir -p data

echo "Running first instance of the database..."
echo "This will create data and write to the WAL file."
go run main.go

echo ""
echo "===================== DATABASE RESTART ====================="
echo ""
echo "Running second instance of the database..."
echo "This should restore data from the WAL and show the persistent records."
go run main.go

echo ""
echo "Check if the test record with ID=2001 was recovered."