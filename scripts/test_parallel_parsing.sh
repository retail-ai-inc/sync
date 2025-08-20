#!/bin/bash

# Test Parallel File Parsing Performance
# This script monitors the performance improvements from parallel parsing

LOG_FILE="/tmp/sync_parallel_test.log"

echo "=== Parallel File Parsing Performance Test ==="
echo "Test started at: $(date)"
echo "Monitoring log file: $LOG_FILE"
echo ""

# Function to analyze performance from logs
analyze_performance() {
    local test_name=$1
    local log_pattern=$2
    
    echo "=== $test_name ==="
    
    # Extract parallel parsing statistics
    echo "Worker Usage:"
    grep "Using.*parallel workers" $LOG_FILE | tail -5
    
    echo ""
    echo "Parsing Performance:"
    grep "Parallel parsing stats" $LOG_FILE | tail -5
    
    echo ""
    echo "Step 2 (Parsing) Performance:"
    grep "Step 2/5.*Parallel file parsing completed" $LOG_FILE | tail -5
    
    echo ""
    echo "Bottleneck Analysis:"
    grep "BOTTLENECK ANALYSIS" $LOG_FILE | tail -5
    
    echo ""
    echo "Overall Performance:"
    grep "Throughput:.*MB/s" $LOG_FILE | tail -5
    
    echo ""
    echo "----------------------------------------"
    echo ""
}

# Monitor performance in real-time
echo "To monitor performance in real-time, run:"
echo "kubectl logs -f sync-pod-name -n mongodb | grep -E 'BatchID|parallel workers|Parallel parsing|BOTTLENECK'"
echo ""

# Performance comparison commands
echo "=== Performance Analysis Commands ==="
echo ""
echo "1. Check parallel worker usage:"
echo "   kubectl logs sync-pod-name -n mongodb | grep 'parallel workers' | tail -10"
echo ""
echo "2. Compare parsing times before/after:"
echo "   # Before (serial): Look for 'Step 2/5.*File parsing completed'"
echo "   # After (parallel): Look for 'Step 2/5.*Parallel file parsing completed'"
echo ""
echo "3. Check individual file parsing stats:"
echo "   kubectl logs sync-pod-name -n mongodb | grep 'Parallel parsing stats' | tail -5"
echo ""
echo "4. Monitor bottleneck changes:"
echo "   kubectl logs sync-pod-name -n mongodb | grep 'BOTTLENECK ANALYSIS' | tail -10"
echo ""
echo "5. Track overall throughput improvements:"
echo "   kubectl logs sync-pod-name -n mongodb | grep 'Throughput:' | tail -10"
echo ""

# Expected improvements
echo "=== Expected Performance Improvements ==="
echo ""
echo "Before (Serial Parsing):"
echo "  - File parsing: ~153ms per file"
echo "  - 116 files: ~17.79 seconds"
echo "  - Step 2 占比: 51.1%"
echo ""
echo "After (Parallel Parsing - 4 workers):"
echo "  - Expected parsing time: ~4.45 seconds (4x improvement)"  
echo "  - Expected Step 2 占比: ~20-25%"
echo "  - Expected total batch time: ~25-28 seconds (vs 34.8s)"
echo ""
echo "Key Metrics to Watch:"
echo "  1. 'Using X parallel workers' - should show 4-8 workers"
echo "  2. 'avg=XXXms' in parallel parsing stats - should be similar to serial"
echo "  3. 'Step 2 (File Parsing): XXXs (XX.X%)' - should be much lower percentage"
echo "  4. 'Total Duration: XXXs' - should be significantly reduced"
echo "  5. 'Throughput: XX.XX MB/s' - should be higher"
echo ""

# Real-time monitoring command
echo "=== Start Real-time Monitoring ==="
echo "Run this command to see parallel parsing in action:"
echo ""
echo "kubectl logs -f \$(kubectl get pods -n mongodb -l app=sync -o jsonpath='{.items[0].metadata.name}') -n mongodb | grep -E 'BatchID.*ItemMasters|parallel workers|Parallel parsing|Step 2/5.*Parallel|BOTTLENECK'"
echo ""