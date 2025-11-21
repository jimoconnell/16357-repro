#!/bin/bash

# Script to run the complete offloadable EP + locks test
# This script:
# 1. Builds the project
# 2. Starts a standalone Hazelcast member with UCN
# 3. Runs the client tests (with locks, then without)
# 4. Cleans up

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "  Offloadable EP + Locks Test Runner"
echo "========================================"
echo ""

# Check for license key
if [ -z "$HZ_LICENSEKEY" ]; then
    echo "⚠️  WARNING: HZ_LICENSEKEY environment variable not set"
    echo "   Please set the HZ_LICENSEKEY environment variable to your Hazelcast license key. From your command line, run:"
    echo "export HZ_LICENSEKEY="V6_UNLI... " (replace with your actual license key)"
    echo
    echo "if you don't have a license key, you can get a free trial from https://hazelcast.com/trial/"
    exit 1
else
    echo "✓ HZ_LICENSEKEY environment variable set"
fi

# Check for existing Hazelcast processes on ports 5701-5703
echo "🔍 Checking for existing Hazelcast processes..."
EXISTING_PIDS=$(lsof -ti :5701 -ti :5702 -ti :5703 2>/dev/null | tr '\n' ' ')
if [ -n "$EXISTING_PIDS" ]; then
    echo "⚠️  Found processes using Hazelcast ports: $EXISTING_PIDS"
    echo "   Killing existing processes..."
    kill $EXISTING_PIDS 2>/dev/null || true
    sleep 2
    # Force kill if still running
    kill -9 $EXISTING_PIDS 2>/dev/null || true
    sleep 1
    echo "✓ Cleaned up existing processes"
else
    echo "✓ No existing processes found"
fi
echo ""

# Build the project
echo "📦 Building project..."
mvn clean package -DskipTests > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "❌ Build failed!"
    exit 1
fi
echo "✓ Build successful"
echo ""

# Start the standalone member in background
echo "🚀 Starting Hazelcast member with User Code Namespaces..."
echo "   Logs will be saved to: member.log"
echo "   Diagnostics enabled"
mvn exec:java -Dexec.mainClass="org.example.StandaloneMemberWithUCN" > member.log 2>&1 &
MEMBER_PID=$!

# Wait for member to start and be ready
echo "   Waiting for member to start and initialize..."
sleep 8

# Check if member is still running
if ! kill -0 $MEMBER_PID 2>/dev/null; then
    echo "❌ Member failed to start. Check member.log for details:"
    tail -20 member.log
    exit 1
fi
echo "✓ Member started (PID: $MEMBER_PID)"
echo ""

# Run the client tests
echo "🧪 Running client tests..."
echo ""
echo "NOTE: Thread names from EntryProcessors will appear in member logs below"
echo ""
mvn exec:java -Dexec.mainClass="org.example.TwoMapLockEpClientReproducer" -Dexec.args="localhost:5701 --offloadable"

CLIENT_EXIT_CODE=$?

# Stop the member
echo ""
echo "🛑 Stopping Hazelcast member..."
kill $MEMBER_PID 2>/dev/null || true
wait $MEMBER_PID 2>/dev/null || true
echo "✓ Member stopped"
echo ""

# Show diagnostics info
echo "========================================"
echo "  Diagnostics Information"
echo "========================================"
if [ -d "diagnostics" ]; then
    echo "Diagnostics directory found:"
    ls -lh diagnostics/ | head -10
    echo ""
    echo "Diagnostic files:"
    find diagnostics -name "*.log" -type f | head -5
    echo ""
else
    echo "No diagnostics directory found (diagnostics may not have been written yet)"
    echo ""
fi

# Show member logs with thread names
echo "========================================"
echo "  Member Logs - Thread Names from EPs"
echo "========================================"
echo "Look for [OffloadableEP] messages showing thread names:"
echo "  - cached.thread-N = OFFLOADED (good)"
echo "  - partition-operation.thread-N = NOT OFFLOADED (locks present)"
echo ""
echo "Thread name output from EntryProcessors and ExecutorService:"
echo ""
echo "TEST 1 (WITH LOCKS) - should show partition-operation threads:"
if grep -a "\[OffloadableEP" member.log | head -20 > /dev/null 2>&1; then
    grep -a "\[OffloadableEP" member.log | head -20
else
    echo "No OffloadableEP output found for TEST 1"
fi
echo ""
echo "TEST 2 (WITHOUT LOCKS) - should show cached threads:"
if grep -a "\[OffloadableEP" member.log | tail -20 > /dev/null 2>&1; then
    # Get middle section (TEST 2) - after TEST 1, before TEST 3
    grep -a "\[OffloadableEP" member.log | tail -20 | head -20
else
    echo "No OffloadableEP output found for TEST 2"
fi
echo ""
echo "TEST 3 (ExecutorService WITH LOCKS) - should show executor/cached threads:"
if grep -a "\[ExecutorServiceTask" member.log > /dev/null 2>&1; then
    grep -a "\[ExecutorServiceTask" member.log | tail -20
else
    echo "No ExecutorServiceTask output found for TEST 3"
fi
echo ""
echo "Summary - counting thread types:"
echo "  Partition-operation threads (NOT OFFLOADED - TEST 1):"
grep -a "partition-operation.thread" member.log | wc -l | xargs echo "    Count:"
echo "  Cached threads (OFFLOADED - TEST 2 & 3):"
grep -a "cached.thread" member.log | wc -l | xargs echo "    Count:"
echo "  ExecutorService tasks (TEST 3):"
grep -a "\[ExecutorServiceTask" member.log | wc -l | xargs echo "    Count:"
echo ""

if [ $CLIENT_EXIT_CODE -eq 0 ]; then
    echo "✅ All tests completed successfully!"
else
    echo "❌ Client tests exited with code: $CLIENT_EXIT_CODE"
    exit $CLIENT_EXIT_CODE
fi

