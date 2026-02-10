#!/bin/bash

# Script to run all tests with both BTree and BPTree index types
# Usage: ./scripts/run_tests_all_indexes.sh [-race] [-v]

set -e

RACE_FLAG=""
VERBOSE_FLAG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -race)
            RACE_FLAG="-race"
            shift
            ;;
        -v)
            VERBOSE_FLAG="-v"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [-race] [-v]"
            exit 1
            ;;
    esac
done

OPTIONS_FILE="options.go"
BACKUP_FILE="options.go.bak"

# Backup original options.go
cp "$OPTIONS_FILE" "$BACKUP_FILE"

# Function to restore original file
cleanup() {
    if [ -f "$BACKUP_FILE" ]; then
        mv "$BACKUP_FILE" "$OPTIONS_FILE"
    fi
}

# Set trap to restore on exit
trap cleanup EXIT

echo "========================================"
echo "Running tests with BTree index (default)"
echo "========================================"

# Set BTree as default
sed -i.tmp 's/IndexType:.*index\.BPTree/IndexType:         index.BTree/' "$OPTIONS_FILE"
sed -i.tmp 's/IndexType:.*index\.BTree/IndexType:         index.BTree/' "$OPTIONS_FILE"
rm -f "$OPTIONS_FILE.tmp"

go clean -testcache
if go test $RACE_FLAG $VERBOSE_FLAG ./...; then
    echo ""
    echo "✅ BTree tests PASSED"
else
    echo ""
    echo "❌ BTree tests FAILED"
    exit 1
fi

echo ""
echo "========================================"
echo "Running tests with BPTree index"
echo "========================================"

# Set BPTree as default
sed -i.tmp 's/IndexType:.*index\.BTree/IndexType:         index.BPTree/' "$OPTIONS_FILE"
rm -f "$OPTIONS_FILE.tmp"

go clean -testcache
if go test $RACE_FLAG $VERBOSE_FLAG ./...; then
    echo ""
    echo "✅ BPTree tests PASSED"
else
    echo ""
    echo "❌ BPTree tests FAILED"
    exit 1
fi

echo ""
echo "========================================"
echo "✅ All tests passed for both index types!"
echo "========================================"
