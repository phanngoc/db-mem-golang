#!/bin/bash

# Exit on error
set -e

echo "Building and running db-mem-golang..."

# Build the Go application
go build -o db-mem-app

# Run the application
./db-mem-app

echo "Application execution completed."