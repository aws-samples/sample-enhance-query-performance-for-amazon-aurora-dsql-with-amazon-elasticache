#!/bin/bash

# Quick Start Script for DSQL + ElastiCache Demo
# This script sets up and runs the performance demonstration

set -e

echo "[START] DSQL + ElastiCache Demo - Quick Start"
echo "=============================================="
echo "Environment: AWS CloudShell"
echo ""

# Function to get user inputs
get_user_inputs() {
    echo "============================================================"
    echo "DSQL + ElastiCache Performance Demo Setup"
    echo "============================================================"
    echo "Please provide your AWS configuration:"
    echo "(Database settings like user='admin', dbname='postgres' are pre-configured)"
    echo ""
    
    # Get AWS Region
    while true; do
        read -p "Enter your AWS region (e.g., us-east-1, us-west-2): " AWS_REGION
        AWS_REGION=$(echo "$AWS_REGION" | xargs)  # trim whitespace
        if [[ -n "$AWS_REGION" ]]; then
            if [[ "$AWS_REGION" =~ ^[a-z]{2}-[a-z]+-[0-9]+$ ]]; then
                break
            else
                echo "[WARNING] This doesn't look like an AWS region. Regions typically look like 'us-east-1'"
                read -p "Continue anyway? (y/n): " confirm
                if [[ "$confirm" =~ ^[Yy]([Ee][Ss])?$ ]]; then
                    break
                fi
            fi
        else
            echo "[ERROR] AWS region cannot be empty. Please try again."
        fi
    done
    
    echo ""
    
    # Get DSQL endpoint
    while true; do
        read -p "Enter your DSQL cluster endpoint: " DSQL_ENDPOINT
        DSQL_ENDPOINT=$(echo "$DSQL_ENDPOINT" | xargs)  # trim whitespace
        if [[ -n "$DSQL_ENDPOINT" ]]; then
            if [[ "$DSQL_ENDPOINT" == *.dsql.*.on.aws ]]; then
                break
            else
                echo "[WARNING] This doesn't look like a DSQL endpoint. DSQL endpoints typically end with '.dsql.{region}.on.aws'"
                read -p "Continue anyway? (y/n): " confirm
                if [[ "$confirm" =~ ^[Yy]([Ee][Ss])?$ ]]; then
                    break
                fi
            fi
        else
            echo "[ERROR] DSQL endpoint cannot be empty. Please try again."
        fi
    done
    
    echo ""
    
    # Get Valkey endpoint
    while true; do
        read -p "Enter your ElastiCache Valkey endpoint (without port): " VALKEY_ENDPOINT
        VALKEY_ENDPOINT=$(echo "$VALKEY_ENDPOINT" | xargs)  # trim whitespace
        
        # Remove :6379 if user included it
        if [[ "$VALKEY_ENDPOINT" == *:6379 ]]; then
            VALKEY_ENDPOINT=${VALKEY_ENDPOINT%:6379}
            echo "[INFO] Removed port suffix. Using: $VALKEY_ENDPOINT"
        fi
        
        if [[ -n "$VALKEY_ENDPOINT" ]]; then
            if [[ "$VALKEY_ENDPOINT" == *cache.amazonaws.com ]]; then
                break
            else
                echo "[WARNING] This doesn't look like an ElastiCache endpoint. ElastiCache endpoints typically contain 'cache.amazonaws.com'"
                read -p "Continue anyway? (y/n): " confirm
                if [[ "$confirm" =~ ^[Yy]([Ee][Ss])?$ ]]; then
                    break
                fi
            fi
        else
            echo "[ERROR] Valkey endpoint cannot be empty. Please try again."
        fi
    done
    
    echo ""
    
    # Get query type
    while true; do
        echo "============================================================"
        echo "Choose execution type:"
        echo "============================================================"
        echo ""
        echo "1. SIMPLE EXECUTION"
        echo "   - Tests basic query: SELECT * FROM users1"
        echo "   - Automatically sets up users1 table"
        echo "   - Quick demo, minimal setup required"
        echo ""
        echo "2. COMPLEX EXECUTION"
        echo "   - Tests complex query with joins and aggregations"
        echo "   - Requires users and orders tables"
        echo "   - Demonstrates real-world caching benefits"
        echo ""
        read -p "Enter your choice (1 or 2): " QUERY_CHOICE
        QUERY_CHOICE=$(echo "$QUERY_CHOICE" | xargs)
        
        if [[ "$QUERY_CHOICE" == "1" ]]; then
            QUERY_TYPE="simple"
            QUERY="SELECT * FROM users1;"
            echo ""
            echo "[SELECTED] Simple execution"
            break
        elif [[ "$QUERY_CHOICE" == "2" ]]; then
            QUERY_TYPE="complex"
            QUERY="SELECT u.user_id, u.name, u.email, u.department, u.role, u.last_login, COUNT(DISTINCT o.order_date) as active_days, COUNT(o.order_id) as recent_orders, COALESCE(SUM(o.order_amount), 0) as recent_spending, COALESCE(AVG(o.order_amount), 0) as avg_order_size, STRING_AGG(DISTINCT o.order_type, ', ') as order_types FROM users u LEFT JOIN orders o ON u.user_id = o.user_id AND o.order_date >= CURRENT_DATE - INTERVAL '30 days' WHERE u.user_id = 1 GROUP BY u.user_id, u.name, u.email, u.department, u.role, u.last_login;"
            echo ""
            echo "[SELECTED] Complex execution"
            break
        else
            echo "[ERROR] Invalid choice. Please enter 1 or 2."
        fi
    done
}

# Get user inputs
get_user_inputs

# Set environment variables from user inputs
export DSQL_ENDPOINT
export VALKEY_ENDPOINT
export AWS_REGION
export VALKEY_TTL="30"
export QUERY

echo ""
echo "[CONFIGURED] AWS Region: $AWS_REGION"
echo "[CONFIGURED] DSQL Endpoint: $DSQL_ENDPOINT"
echo "[CONFIGURED] Valkey Endpoint: $VALKEY_ENDPOINT"
echo "[CONFIGURED] Database: postgres (admin user) - pre-configured"
echo "[CONFIGURED] Cache TTL: $VALKEY_TTL seconds"
echo "[CONFIGURED] Query Type: $QUERY_TYPE"
echo ""

echo "[INSTALL] Installing Python dependencies..."
python3 -m pip install --user redis psycopg2-binary boto3 --quiet
echo "[OK] Dependencies installed"
echo ""

# Setup database based on query type
if [[ "$QUERY_TYPE" == "simple" ]]; then
    echo "[DATABASE] Setting up users1 table for simple execution..."
    python3 setup_database.py
    echo ""
elif [[ "$QUERY_TYPE" == "complex" ]]; then
    echo "[DATABASE] Complex execution requires users and orders tables"
    echo "[INFO] Ensure these tables exist before proceeding"
    read -p "Press Enter to continue or Ctrl+C to cancel..."
    echo ""
fi

echo "[TEST] Running comprehensive performance test with connection pooling..."
echo "=================================================="
echo "[INFO] This will run all three scenarios in a single execution to demonstrate"
echo "[INFO] true connection pooling benefits across multiple test runs"

# Capture performance results from each scenario
RESULTS=$(python3 -c "
import sys
import json
sys.path.append('.')
from cloudshell_dsql_elasticache import main
import time

print('[SCENARIO 1] DSQL Cold Start - First Test')
result1 = main('$DSQL_ENDPOINT', '$VALKEY_ENDPOINT', '$QUERY_TYPE')
print('')

print('[WAIT] Waiting 5 seconds...')
time.sleep(5)
print('')

print('[SCENARIO 2] DSQL Warm - Second Test (same process, pool maintained)')
result2 = main('$DSQL_ENDPOINT', '$VALKEY_ENDPOINT', '$QUERY_TYPE')
print('')

print('[WAIT] Waiting 3 seconds...')
time.sleep(3)
print('')

print('[SCENARIO 3] DSQL Optimized - Third Test (same process, pool maintained)')
result3 = main('$DSQL_ENDPOINT', '$VALKEY_ENDPOINT', '$QUERY_TYPE')
print('')

# Clean up connection pool only after all scenarios are complete
from cloudshell_dsql_elasticache import cleanup_connection_pool
cleanup_connection_pool()

print('[COMPLETE] All three scenarios completed in single process with persistent connection pool')

# Output results as JSON for shell script to parse
print('RESULTS_JSON:' + json.dumps({
    'run1': result1,
    'run2': result2, 
    'run3': result3
}))
")

# Extract the JSON results
PERF_JSON=$(echo "$RESULTS" | grep "RESULTS_JSON:" | sed 's/RESULTS_JSON://')

# Parse results using Python
PERF_SUMMARY=$(python3 -c "
import json
import sys
data = json.loads('$PERF_JSON')
r1, r2, r3 = data['run1'], data['run2'], data['run3']
print(f'{r1[\"dsql_time_ms\"]}|{r1[\"cache_avg_ms\"]}|{r2[\"dsql_time_ms\"]}|{r2[\"cache_avg_ms\"]}|{r3[\"dsql_time_ms\"]}|{r3[\"cache_avg_ms\"]}')
")

# Split the results
IFS='|' read -r RUN1_DSQL RUN1_CACHE RUN2_DSQL RUN2_CACHE RUN3_DSQL RUN3_CACHE <<< "$PERF_SUMMARY"
echo ""

echo "[COMPLETE] Demo complete! The results above demonstrate:"
echo "  • Amazon Aurora DSQL: Automatic scaling with variable cold start latency"
echo "  • DSQL Connection Pooling: $(python3 -c "print(f'{${RUN1_DSQL}/${RUN3_DSQL}:.0f}x')")performance improvement (${RUN1_DSQL}ms → ${RUN3_DSQL}ms)"
echo "  • DSQL Best Practices: Connection pooling + token caching essential for performance"
echo "  • Cache Strategy: Eliminates DSQL variability with predictable ~1-2ms response"
echo ""
echo "[KEY INSIGHT] DSQL benefits tremendously from caching:"
echo "              - DSQL: Variable ${RUN3_DSQL}-${RUN1_DSQL}ms+ depending on compute state"
echo "              - ElastiCache: Consistent ${RUN1_CACHE}-${RUN3_CACHE}ms regardless of DSQL scaling"
echo "              - Result: Predictable user experience despite DSQL variability"
echo ""
echo "============================================================"
echo "AMAZON AURORA DSQL + CACHING PERFORMANCE SUMMARY"
echo "============================================================"
echo "Amazon Aurora DSQL is a distributed SQL database that"
echo "automatically scales compute and storage based on demand."
echo ""
echo "Database Performance (actual observed results):"
echo ""
echo "Run 1 - DSQL Cold Start:"
echo "  Cache Miss:  ${RUN1_DSQL}ms (DSQL compute initialization + new connections)"
echo "  Cache Hits:  ${RUN1_CACHE}ms (consistent ElastiCache performance)"
echo ""
echo "Run 2 - DSQL Warm (Connection Pool):"  
echo "  Cache Miss:  ${RUN2_DSQL}ms (DSQL compute warmed + pooled connections)"
echo "  Cache Hits:  ${RUN2_CACHE}ms (consistent ElastiCache performance)"
echo ""
echo "Run 3 - DSQL Optimized (Connection Pool):"
echo "  Cache Miss:  ${RUN3_DSQL}ms (DSQL compute optimized + pooled connections)"
echo "  Cache Hits:  ${RUN3_CACHE}ms (consistent ElastiCache performance)"
echo ""
echo "Database Insights:"
echo "  ✓ DSQL scaling: ${RUN1_DSQL}ms → ${RUN3_DSQL}ms ($(python3 -c "print(f'{${RUN1_DSQL}/${RUN3_DSQL}:.0f}x')")improvement with connection pooling)"
echo "  ✓ DSQL variability: Cold starts can be unpredictable (100-300ms+)"
echo "  ✓ Connection pooling: Essential for DSQL to minimize overhead"
echo "  ✓ Caching strategy: Eliminates DSQL variability with consistent 1-2ms response"
echo "  ✓ Best practice: Cache + connection pooling = predictable DSQL performance"
echo "============================================================"
echo ""
echo "[INFO] To run individual tests:"
echo "   python3 cloudshell_dsql_elasticache.py"
echo ""
echo "[REPEAT] To run the demo again:"
echo "   ./quick_start.sh"
echo ""
echo "[FILES] Key files:"
echo "   - cloudshell_dsql_elasticache.py - Main test script"
echo "   - setup_database.py - Database setup script"
echo "   - quick_start.sh - Complete automated demo"
