#!/usr/bin/env python3
"""
DSQL and ElastiCache Integration Example - CloudShell Ready
 
This script demonstrates how to use Amazon ElastiCache (Valkey) as a caching layer
in front of Amazon DSQL (PostgreSQL) for improved query performance.

Enhanced with connection pooling for better performance and resource management.
Optimized for AWS CloudShell environment.

INTERACTIVE MODE:
-----------------
The script will prompt you to choose between:

1. SIMPLE EXECUTION:
   - Tests basic table scan: SELECT * FROM users1
   - Automatically creates the users1 table if it doesn't exist
   - Good for basic performance testing and demos
   
2. COMPLEX EXECUTION:
   - Tests joins and aggregations across users and orders tables
   - Requires both users and orders tables with sample data
   - Demonstrates caching benefits for complex analytical queries

USAGE:
------
Simply run the script and follow the prompts:
python3 cloudshell_dsql_elasticache.py
"""

import os
import sys
import time
import logging
import datetime
import threading
import warnings
from typing import Optional, Tuple, Any, List
from queue import Queue, Empty

# Suppress Python deprecation warnings from boto3
warnings.filterwarnings('ignore', category=DeprecationWarning, module='boto3')

# Packages installed by quick_start.sh

import boto3
import psycopg2
from psycopg2 import pool
import redis
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Configuration - CloudShell optimized
CONFIG = {
    'valkey': {
        'url': os.environ.get('VALKEY_URL', 'valkey://localhost:6379'),  # Will be updated with actual endpoint
        'ttl': int(os.environ.get('VALKEY_TTL', '30')),  # Cache TTL in seconds (30 seconds for 9 iterations)
    },
    'dsql': {
        'region': os.environ.get('AWS_REGION', 'us-east-1'),
        'dbname': 'postgres',
        'user': 'admin',
        'ssl_mode': 'require',  # Changed from verify-full to require for compatibility
        'ssl_root_cert': None,  # Don't specify cert path to avoid SSL verification issues
    },
    'connection_pool': {
        'min_connections': int(os.environ.get('DSQL_POOL_MIN', '5')),  # Minimum connections in pool
        'max_connections': int(os.environ.get('DSQL_POOL_MAX', '30')), # Maximum connections in pool
        'connection_timeout': int(os.environ.get('DSQL_POOL_TIMEOUT', '30')), # Connection timeout in seconds
    },
    'queries': {
        'simple': 'SELECT * FROM users1;',
        'complex': 'SELECT u.user_id, u.name, u.email, u.department, u.role, u.last_login, COUNT(DISTINCT o.order_date) as active_days, COUNT(o.order_id) as recent_orders, COALESCE(SUM(o.order_amount), 0) as recent_spending, COALESCE(AVG(o.order_amount), 0) as avg_order_size, STRING_AGG(DISTINCT o.order_type, \', \') as order_types FROM users u LEFT JOIN orders o ON u.user_id = o.user_id AND o.order_date >= CURRENT_DATE - INTERVAL \'30 days\' WHERE u.user_id = 1 GROUP BY u.user_id, u.name, u.email, u.department, u.role, u.last_login;'
    }
}


# Global connection pool instance
dsql_connection_pool = None


class DSQLConnectionPool:
    """
    Connection pool for DSQL using psycopg2's ThreadedConnectionPool.
    Generates a fresh token for each connection request.
    """
    
    def __init__(self, cluster_endpoint: str, region: str):
        """
        Initialize the connection pool.
        
        Args:
            cluster_endpoint: DSQL cluster endpoint
            region: AWS region
        """
        self.cluster_endpoint = cluster_endpoint
        self.region = region
        self.pool = None
        self.lock = threading.Lock()
        
        # Initialize the pool
        self._create_pool()
        
        logger.info(f"[POOL] Initialized DSQL connection pool (min={CONFIG['connection_pool']['min_connections']}, max={CONFIG['connection_pool']['max_connections']})")
    
    def _generate_auth_token(self) -> str:
        """Generate a fresh DSQL authentication token."""
        try:
            # Generate new token
            dsql_client = boto3.client('dsql', region_name=self.region)
            password_token = dsql_client.generate_db_connect_admin_auth_token(
                self.cluster_endpoint,
                self.region
            )
            
            logger.info(f"[POOL] Generated fresh auth token")
            return password_token
        except Exception as e:
            logger.error(f"[POOL] Failed to generate auth token: {e}")
            raise
    
    def _create_pool(self):
        """Create the psycopg2 connection pool with fresh token generation."""
        try:
            # Create a custom connection factory that generates fresh tokens
            def connection_factory(*args, **kwargs):
                # Generate fresh token for each connection
                kwargs['password'] = self._generate_auth_token()
                return psycopg2.connect(*args, **kwargs)
            
            # Connection parameters (without password - will be added by factory)
            self.conn_params = {
                'dbname': CONFIG['dsql']['dbname'],
                'user': CONFIG['dsql']['user'],
                'host': self.cluster_endpoint,
                'sslmode': CONFIG['dsql']['ssl_mode'],
            }
            
            # Only add sslrootcert if it's specified
            if CONFIG['dsql']['ssl_root_cert']:
                self.conn_params['sslrootcert'] = CONFIG['dsql']['ssl_root_cert']
            
            # Create threaded connection pool with custom connection factory
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=CONFIG['connection_pool']['min_connections'],
                maxconn=CONFIG['connection_pool']['max_connections'],
                connection_factory=connection_factory,
                **self.conn_params
            )
            
            logger.info(f"[POOL] Created connection pool with fresh token generation")
            
        except Exception as e:
            logger.error(f"[POOL] Failed to create connection pool: {e}")
            raise
    
    def get_connection(self):
        """
        Get a connection from the pool.
        
        Returns:
            A database connection from the pool
        """
        try:
            if not self.pool:
                raise Exception("Connection pool not initialized")
            
            # Get connection from pool - fresh token is generated for each new connection
            conn = self.pool.getconn()
            
            if conn:
                # Test the connection
                if self._test_connection(conn):
                    logger.debug(f"[POOL] Retrieved valid connection from pool")
                    return conn
                else:
                    # Connection is invalid, return it and get a new one
                    logger.debug(f"[POOL] Connection invalid, getting fresh connection")
                    self.pool.putconn(conn, close=True)
                    conn = self.pool.getconn()
                    return conn
            else:
                raise Exception("Failed to get connection from pool")
                
        except Exception as e:
            logger.error(f"[POOL] Error getting connection: {e}")
            raise
    
    def return_connection(self, conn):
        """
        Return a connection to the pool.
        
        Args:
            conn: The database connection to return
        """
        try:
            if conn and self.pool:
                # Reset connection state
                if not conn.closed:
                    conn.rollback()
                self.pool.putconn(conn)
                logger.debug(f"[POOL] Returned connection to pool")
        except Exception as e:
            logger.warning(f"[POOL] Error returning connection to pool: {e}")
    
    def _test_connection(self, conn) -> bool:
        """
        Test if a connection is still valid.
        
        Args:
            conn: The database connection to test
            
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            if conn.closed:
                return False
            
            # Quick test query
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            return True
        except Exception:
            return False
    
    def close_all(self):
        """Close all connections in the pool."""
        try:
            if self.pool:
                self.pool.closeall()
                logger.info("[POOL] Closed all connections in pool")
        except Exception as e:
            logger.warning(f"[POOL] Error closing connection pool: {e}")


def get_dsql_connection_pool(cluster_endpoint: str, region: str) -> DSQLConnectionPool:
    """
    Get or create the global DSQL connection pool.
    
    Args:
        cluster_endpoint: DSQL cluster endpoint
        region: AWS region
        
    Returns:
        DSQLConnectionPool instance
    """
    global dsql_connection_pool
    
    if dsql_connection_pool is None:
        dsql_connection_pool = DSQLConnectionPool(cluster_endpoint, region)
    
    return dsql_connection_pool


def create_valkey_client(valkey_endpoint: str) -> redis.Redis:
    """Create and return a Valkey client."""
    try:
        # Create TLS connection (matches valkey-cli --tls method)
        client = redis.Redis(
            host=valkey_endpoint,
            port=6379,
            ssl=True,
            ssl_check_hostname=False,
            ssl_cert_reqs=None,
            socket_connect_timeout=10,
            socket_timeout=10
        )
        # Test connection
        client.ping()
        logger.info(f"[OK] Successfully connected to Valkey at {valkey_endpoint}")
        return client
    except redis.RedisError as e:
        logger.error(f"[ERROR] Error connecting to Valkey: {e}")
        raise


def get_dsql_client(region: str) -> boto3.client:
    """Create and return a DSQL client."""
    try:
        return boto3.client("dsql", region_name=region)
    except ClientError as e:
        logger.error(f"[ERROR] Error creating DSQL client: {e}")
        raise


def execute_dsql_query(cluster_endpoint: str, query: str) -> Tuple[float, Any]:
    """
    Execute a query against DSQL using connection pooling and measure execution time.
    
    Args:
        cluster_endpoint: The DSQL cluster endpoint
        query: The SQL query to execute
        
    Returns:
        Tuple containing (execution_time_delta, query_results)
    """
    try:
        # Get connection pool
        pool = get_dsql_connection_pool(cluster_endpoint, CONFIG['dsql']['region'])
        
        # Get connection from pool
        conn = pool.get_connection()
        
        logger.info(f"[CONNECT] Using pooled connection to DSQL cluster: {cluster_endpoint}")
        
        try:
            # Execute query and measure time
            cur = conn.cursor()
            start = datetime.datetime.now()
            logger.info(f"[QUERY] Executing query in DSQL: {query}")
            logger.info(f"[TIME] Query start time: {start}")
            
            cur.execute(query)
            data = cur.fetchall()
            
            end = datetime.datetime.now()
            delta = end - start
            logger.info(f"[TIME] Query end time: {end}")
            logger.info(f"[TIME] DSQL execution time: {delta}")
            
            # Format result for cache storage
            result_str = ''.join(str(v) for v in data).replace('(', '').replace(')', '')
            
            cur.close()
            logger.info(f"[OK] Successfully executed DSQL query using connection pool, returned {len(data)} rows")
            
            return delta, result_str
            
        finally:
            # Always return connection to pool
            pool.return_connection(conn)
        
    except psycopg2.Error as e:
        logger.error(f"[ERROR] PostgreSQL Error: {e}")
        raise
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
        raise


def get_from_cache(cache: redis.Redis, key: str) -> Tuple[Optional[str], datetime.timedelta, Optional[datetime.timedelta]]:
    """
    Try to get result from cache and measure access time.
    
    Args:
        cache: Valkey client
        key: Cache key to look up
        
    Returns:
        Tuple of (result_or_None, cache_access_time, original_dsql_time_or_None)
        - If cache hit: (result, cache_time, original_dsql_time)
        - If cache miss: (None, cache_time, None)
    """
    start = datetime.datetime.now()
    result = cache.get(key)
    end = datetime.datetime.now()
    delta = end - start
    
    if result:
        logger.info(f"[CACHE HIT] Retrieved data in {delta}")
        import json
        cache_data = json.loads(result.decode())
        query_result = cache_data['result']
        dsql_time_seconds = cache_data['dsql_time_seconds']
        original_dsql_time = datetime.timedelta(seconds=dsql_time_seconds)
        logger.info(f"[TIMING] Retrieved original DSQL time: {original_dsql_time}")
        return query_result, delta, original_dsql_time
    else:
        logger.info(f"[CACHE MISS] Cache access time: {delta}")
    return None, delta, None


def print_performance_summary(cache_time: datetime.timedelta, dsql_time: datetime.timedelta):
    """Print a nice performance comparison summary."""
    print("\n" + "="*60)
    print("PERFORMANCE COMPARISON SUMMARY")
    print("="*60)
    print(f"DSQL query time:        {dsql_time}")
    print(f"Cache access time:      {cache_time}")
    
    if cache_time.total_seconds() > 0:
        speedup = dsql_time.total_seconds() / cache_time.total_seconds()
        print(f"Cache speedup:          {speedup:.2f}x faster")
        
        # Calculate percentage improvement
        improvement = ((dsql_time.total_seconds() - cache_time.total_seconds()) / dsql_time.total_seconds()) * 100
        print(f"Performance improvement: {improvement:.1f}%")
    
    print("="*60)


def hydrate_cache(cache: redis.Redis, key: str, value: str, dsql_time: datetime.timedelta, ttl: int) -> None:
    """
    Store data in the Valkey cache with a specified TTL, including DSQL timing metadata.

    Args:
        cache: Valkey client
        key: Cache key to store the data under
        value: Data to store in the cache
        dsql_time: The original DSQL query execution time
        ttl: Time-to-live in seconds
    """
    try:
        # Store both the result and the DSQL timing as JSON
        import json
        cache_data = {
            'result': value,
            'dsql_time_seconds': dsql_time.total_seconds()
        }
        cache.setex(key, ttl, json.dumps(cache_data))
        logger.info(f"[OK] Cache hydration successful for key '{key}'")
        logger.info(f"[TTL] Data will expire after {ttl} seconds")
        logger.info(f"[TIMING] Stored DSQL time: {dsql_time} ({dsql_time.total_seconds():.4f}s)")
    except redis.RedisError as e:
        logger.error(f"[ERROR] Failed to hydrate cache: {e}")
        raise


def main(cluster_endpoint: str, valkey_endpoint: str, query_type: str) -> dict:
    """
    Main function to demonstrate caching with ElastiCache and DSQL.
    
    Runs complete demo in one execution:
    - Iteration 1: Cache miss, executes DSQL query, hydrates cache
    - Iterations 2-10: Cache hits, demonstrates performance improvement
    
    Args:
        cluster_endpoint: DSQL cluster endpoint
        valkey_endpoint: ElastiCache Valkey endpoint
        query_type: 'simple' or 'complex'
        
    Returns:
        Dictionary with performance metrics for summary
    """
    # Select query based on query type
    query = CONFIG['queries'].get(query_type, CONFIG['queries']['complex'])
    
    num_cache_hits = 9
    cache_hit_times = []
    cache_miss_time = None
    
    print("\n[START] Starting DSQL ElastiCache Performance Demo with Connection Pooling")
    print(f"DSQL Endpoint: {cluster_endpoint}")
    print(f"Valkey Endpoint: {valkey_endpoint}")
    print(f"Connection Pool: min={CONFIG['connection_pool']['min_connections']}, max={CONFIG['connection_pool']['max_connections']}")
    print(f"Query Type: {query_type.upper()}")
    print(f"Query: {query}")
    print("-" * 60)
    
    try:
        # Initialize Valkey client
        cache = create_valkey_client(valkey_endpoint)
        
        # Always start fresh - clear any existing cache for this demo
        print("\n[CLEAR] Clearing cache to start fresh demo...")
        try:
            cache.delete(query)
            print("[OK] Cache cleared")
        except:
            print("[INFO] No existing cache to clear")
        
        # ITERATION 1: Cache Miss (Execute DSQL)
        print("\n[CACHE MISS - ITERATION 1/10]")
        sys.stdout.flush()
        dsql_time, result = execute_dsql_query(cluster_endpoint, query)
        cache_miss_time = dsql_time
        hydrate_cache(cache, query, result, dsql_time, CONFIG['valkey']['ttl'])
        print(f"[INFO] Cache hydrated. DSQL time: {cache_miss_time}")
        sys.stdout.flush()
        
        # ITERATIONS 2-10: Cache Hits
        for i in range(2, num_cache_hits + 2):
            print(f"\n[CACHE HIT - ITERATION {i}/10]")
            sys.stdout.flush()
            cache_result, cache_time, original_dsql_time = get_from_cache(cache, query)
            if cache_result:
                print("[OK] Query result fetched from ElastiCache")
                print(f"Cache access time: {cache_time}")
                sys.stdout.flush()
                cache_hit_times.append(cache_time)
                if original_dsql_time:
                    speedup = original_dsql_time.total_seconds() / cache_time.total_seconds()
                    improvement = ((original_dsql_time.total_seconds() - cache_time.total_seconds()) / original_dsql_time.total_seconds()) * 100
                    print(f"Speedup: {speedup:.2f}x faster | Improvement: {improvement:.1f}%")
                else:
                    print("[WARNING] Original DSQL time not found in cache")
                sys.stdout.flush()
            else:
                print("[WARNING] Unexpected cache miss during hit iterations. Rehydrating...")
                sys.stdout.flush()
                dsql_time, result = execute_dsql_query(cluster_endpoint, query)
                hydrate_cache(cache, query, result, dsql_time, CONFIG['valkey']['ttl'])
        
        # Print comprehensive summary
        print("\n" + "="*60)
        print("PERFORMANCE SUMMARY - COMPLETE DEMO WITH CONNECTION POOLING")
        print("="*60)
        if cache_miss_time:
            print(f"Cache Miss (DSQL):            {cache_miss_time.total_seconds():.6f}s ({cache_miss_time.total_seconds()*1000:.1f}ms)")
        if cache_hit_times:
            avg_cache_hit = sum(t.total_seconds() for t in cache_hit_times) / len(cache_hit_times)
            min_cache_hit = min(t.total_seconds() for t in cache_hit_times)
            max_cache_hit = max(t.total_seconds() for t in cache_hit_times)
            print(f"\nCache Hits ({len(cache_hit_times)} iterations):")
            print(f"  Average:  {avg_cache_hit:.6f}s ({avg_cache_hit*1000:.1f}ms)")
            print(f"  Min:      {min_cache_hit:.6f}s ({min_cache_hit*1000:.1f}ms)")
            print(f"  Max:      {max_cache_hit:.6f}s ({max_cache_hit*1000:.1f}ms)")
            
            if cache_miss_time:
                speedup = cache_miss_time.total_seconds() / avg_cache_hit
                improvement = ((cache_miss_time.total_seconds() - avg_cache_hit) / cache_miss_time.total_seconds()) * 100
                print(f"\n" + "-"*60)
                print(f"PERFORMANCE IMPROVEMENT:")
                print(f"  DSQL:     {cache_miss_time.total_seconds()*1000:.1f}ms")
                print(f"  Cache:    {avg_cache_hit*1000:.1f}ms")
                print(f"  Speedup:  {speedup:.1f}x faster")
                print(f"  Improvement: {improvement:.1f}%")
        print("="*60)
        sys.stdout.flush()
        
        # Return performance metrics for summary
        return {
            'dsql_time_ms': round(cache_miss_time.total_seconds() * 1000, 1),
            'cache_avg_ms': round(avg_cache_hit * 1000, 1),
            'cache_min_ms': round(min_cache_hit * 1000, 1),
            'cache_max_ms': round(max_cache_hit * 1000, 1),
            'speedup': round(speedup, 1),
            'improvement': round(improvement, 1)
        }
        
    except Exception as e:
        logger.error(f"[ERROR] Error in main function: {e}")
        print(f"\n[TROUBLESHOOT] Troubleshooting tips:")
        print(f"   • Ensure you're running from CloudShell")
        print(f"   • Verify ElastiCache endpoint is reachable")
        print(f"   • Check DSQL cluster endpoint and permissions")
        print(f"   • Make sure the required tables exist in the database:")
        print(f"     - For QUERY_TYPE=simple: users1 table (run setup_database.py)")
        print(f"     - For QUERY_TYPE=complex: users and orders tables with sample data")
        print(f"   • Current query type: {query_type}")
        sys.exit(1)
    # Note: Connection pool is NOT closed here - it persists across main() calls




def cleanup_connection_pool():
    """Clean up the global connection pool - call this only at the very end."""
    global dsql_connection_pool
    if dsql_connection_pool:
        dsql_connection_pool.close_all()
        dsql_connection_pool = None
        logger.info("[CLEANUP] Connection pool closed and reset")


def prompt_query_type() -> str:
    """
    Prompt the user to choose between simple or complex execution.
    
    Returns:
        'simple' or 'complex'
    """
    print("\n" + "="*60)
    print("DSQL ELASTICACHE PERFORMANCE DEMO")
    print("="*60)
    print("\nPlease choose your execution type:\n")
    print("1. SIMPLE EXECUTION")
    print("   - Tests basic query: SELECT * FROM users1")
    print("   - Automatically sets up users1 table")
    print("   - Quick demo, minimal setup required")
    print()
    print("2. COMPLEX EXECUTION")
    print("   - Tests complex query with joins and aggregations")
    print("   - Requires users and orders tables")
    print("   - Demonstrates real-world caching benefits")
    print()
    
    while True:
        choice = input("Enter your choice (1 or 2): ").strip()
        if choice == '1':
            print("\n[SELECTED] Simple execution")
            return 'simple'
        elif choice == '2':
            print("\n[SELECTED] Complex execution")
            return 'complex'
        else:
            print("[ERROR] Invalid choice. Please enter 1 or 2.")


def setup_users1_table(cluster_endpoint: str, region: str) -> bool:
    """
    Create and populate the users1 table for simple query testing.
    
    Args:
        cluster_endpoint: DSQL cluster endpoint
        region: AWS region
        
    Returns:
        True if successful, False otherwise
    """
    print("\n[SETUP] Setting up users1 table for simple query testing...")
    
    try:
        # Get connection pool
        pool = get_dsql_connection_pool(cluster_endpoint, region)
        conn = pool.get_connection()
        
        try:
            cur = conn.cursor()
            
            # Check if table already exists
            print("[CHECK] Checking if users1 table exists...")
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'users1'
                );
            """)
            table_exists = cur.fetchone()[0]
            
            if table_exists:
                print("[EXISTS] users1 table already exists")
                
                # Check row count
                cur.execute("SELECT COUNT(*) FROM users1;")
                row_count = cur.fetchone()[0]
                print(f"[COUNT] Current row count: {row_count}")
                
                if row_count > 0:
                    print("[OK] Table already has data, ready to proceed")
                    cur.close()
                    return True
                else:
                    print("[EMPTY] Table is empty, adding test data...")
            else:
                print("[CREATE] Creating users1 table...")
                
                # Create the table
                create_table_sql = """
                CREATE TABLE users1 (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    age INTEGER,
                    department VARCHAR(50),
                    salary DECIMAL(10,2),
                    hire_date DATE,
                    is_active BOOLEAN
                );
                """
                
                cur.execute(create_table_sql)
                print("[OK] users1 table created successfully")
            
            # Insert test data
            print("[INSERT] Inserting test data...")
            
            insert_sql = """
            INSERT INTO users1 (id, name, email, age, department, salary, hire_date, is_active) VALUES
            (1, 'John Doe', 'john.doe@company.com', 30, 'Engineering', 75000.00, '2022-01-15', true),
            (2, 'Jane Smith', 'jane.smith@company.com', 28, 'Marketing', 65000.00, '2022-02-20', true),
            (3, 'Mike Johnson', 'mike.johnson@company.com', 35, 'Engineering', 85000.00, '2021-11-10', true),
            (4, 'Sarah Wilson', 'sarah.wilson@company.com', 32, 'Sales', 70000.00, '2022-03-05', true),
            (5, 'David Brown', 'david.brown@company.com', 29, 'Engineering', 78000.00, '2022-01-25', true),
            (6, 'Lisa Garcia', 'lisa.garcia@company.com', 31, 'HR', 62000.00, '2022-04-12', true),
            (7, 'Tom Davis', 'tom.davis@company.com', 27, 'Marketing', 58000.00, '2022-05-18', true),
            (8, 'Emma Martinez', 'emma.martinez@company.com', 33, 'Engineering', 82000.00, '2021-12-08', true),
            (9, 'Chris Anderson', 'chris.anderson@company.com', 26, 'Sales', 67000.00, '2022-06-22', true),
            (10, 'Amy Taylor', 'amy.taylor@company.com', 34, 'Engineering', 88000.00, '2021-10-15', true)
            ON CONFLICT (email) DO NOTHING;
            """
            
            cur.execute(insert_sql)
            
            # Check how many rows were inserted
            cur.execute("SELECT COUNT(*) FROM users1;")
            total_rows = cur.fetchone()[0]
            print(f"[OK] Test data inserted successfully. Total rows: {total_rows}")
            
            # Show sample of the data
            print("\n[SAMPLE] Sample data from users1 table:")
            cur.execute("SELECT id, name, email, department FROM users1 LIMIT 3;")
            sample_data = cur.fetchall()
            
            for row in sample_data:
                print(f"   ID: {row[0]}, Name: {row[1]}, Email: {row[2]}, Dept: {row[3]}")
            
            print(f"\n[READY] Database setup complete! Ready for performance testing.")
            cur.close()
            return True
            
        finally:
            pool.return_connection(conn)
            
    except Exception as e:
        logger.error(f"[ERROR] Error setting up database: {e}")
        print(f"[ERROR] Failed to setup users1 table: {e}")
        return False


if __name__ == "__main__":
    # Check for command line arguments first
    if len(sys.argv) >= 3:
        # Support both old format (2 args) and new format (3+ args)
        if len(sys.argv) >= 4:
            region = sys.argv[1]
            cluster_endpoint = sys.argv[2]
            valkey_endpoint = sys.argv[3]
            # Check if query_type is provided (for quick_start.sh automation)
            query_type = sys.argv[4] if len(sys.argv) >= 5 else None
        else:
            # Legacy: 2 arguments (no region specified)
            cluster_endpoint = sys.argv[1]
            valkey_endpoint = sys.argv[2]
            region = os.environ.get('AWS_REGION', 'us-east-1')  # fallback
            query_type = None
        
        print(f"[ARGS] Using command line arguments:")
        print(f"[ARGS] AWS Region: {region}")
        print(f"[ARGS] DSQL Endpoint: {cluster_endpoint}")
        print(f"[ARGS] Valkey Endpoint: {valkey_endpoint}")
    else:
        # Check for environment variables (primary method)
        region = os.environ.get('AWS_REGION')
        cluster_endpoint = os.environ.get('DSQL_ENDPOINT')
        valkey_endpoint = os.environ.get('VALKEY_ENDPOINT')
        query_type = None  # Will prompt interactively
        
        if region and cluster_endpoint and valkey_endpoint:
            print(f"[ENV] Using environment variables:")
            print(f"[ENV] AWS Region: {region}")
            print(f"[ENV] DSQL Endpoint: {cluster_endpoint}")
            print(f"[ENV] Valkey Endpoint: {valkey_endpoint}")
        else:
            # Error: Required configuration missing
            print(f"[ERROR] Missing required configuration!")
            print(f"[ERROR]")
            print(f"[ERROR] Please run the quick start script:")
            print(f"[ERROR]   ./quick_start.sh")
            print(f"[ERROR]")
            print(f"[ERROR] The script will prompt for your endpoints and handle all setup automatically.")
            sys.exit(1)
    
    # Update CONFIG with the provided region
    CONFIG['dsql']['region'] = region
    
    print(f"[START] DSQL ElastiCache Performance Test")
    print(f"Timestamp: {datetime.datetime.now()}")
    
    # Only prompt for query type if not provided (interactive mode)
    if query_type is None:
        query_type = prompt_query_type()
        
        # If simple execution, setup the users1 table
        if query_type == 'simple':
            success = setup_users1_table(cluster_endpoint, region)
            if not success:
                print("\n[ERROR] Failed to setup database. Cannot proceed.")
                sys.exit(1)
        else:
            print("\n[INFO] Complex execution selected.")
            print("[INFO] Ensure users and orders tables exist with sample data.")
            proceed = input("\nProceed with complex query test? (y/n): ").strip().lower()
            if proceed != 'y':
                print("[CANCELLED] Test cancelled by user.")
                sys.exit(0)
    else:
        # Automated mode (called from quick_start.sh)
        print(f"\n[AUTOMATED] Running in automated mode with query type: {query_type}")
    
    # Run the performance test
    main(cluster_endpoint, valkey_endpoint, query_type)
