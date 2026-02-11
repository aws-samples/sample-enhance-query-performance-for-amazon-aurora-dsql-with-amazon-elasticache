#!/usr/bin/env python3
"""
DSQL Database Setup Script
Creates the users1 table and populates it with test data for the ElastiCache demo.
"""

import os
import sys
import logging
import datetime
import warnings

# Suppress Python deprecation warnings from boto3
warnings.filterwarnings('ignore', category=DeprecationWarning, module='boto3')

# Packages installed by quick_start.sh

import boto3
import psycopg2
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'dsql': {
        'region': os.environ.get('AWS_REGION', 'us-east-1'),
        'dbname': 'postgres',
        'user': 'admin',
        'ssl_mode': 'prefer',  # Use prefer for CloudShell compatibility
        'ssl_root_cert': None,  # Don't specify cert path for CloudShell
    }
}

def create_dsql_connection(cluster_endpoint: str):
    """Create a connection to the DSQL cluster."""
    try:
        # Get DSQL client and generate auth token
        client = boto3.client("dsql", region_name=CONFIG['dsql']['region'])
        password_token = client.generate_db_connect_admin_auth_token(
            cluster_endpoint, 
            CONFIG['dsql']['region']
        )
        
        # Connection parameters
        conn_params = {
            'dbname': CONFIG['dsql']['dbname'],
            'user': CONFIG['dsql']['user'],
            'host': cluster_endpoint,
            'sslmode': CONFIG['dsql']['ssl_mode'],
            'password': password_token
        }
        
        # Only add sslrootcert if it's specified
        if CONFIG['dsql']['ssl_root_cert']:
            conn_params['sslrootcert'] = CONFIG['dsql']['ssl_root_cert']
        
        print(f"[CONNECT] Connecting to DSQL cluster: {cluster_endpoint}")
        conn = psycopg2.connect(**conn_params)
        conn.set_session(autocommit=True)
        print("[OK] Successfully connected to DSQL")
        return conn
        
    except Exception as e:
        print(f"[ERROR] Error connecting to DSQL: {e}")
        raise

def setup_simple_database(cur):
    """Set up users1 table for simple queries."""
    print("\n[SETUP] Setting up users1 table for simple queries...")
    
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
        
        if row_count == 0:
            print("[EMPTY] Table is empty, adding test data...")
        else:
            print("[OK] Table already has data")
            # Show sample data
            cur.execute("SELECT * FROM users1 LIMIT 5;")
            sample_data = cur.fetchall()
            print("[SAMPLE] Sample data:")
            for row in sample_data:
                print(f"   {row}")
            return
    else:
        print("[CREATE] Creating users1 table...")
        
        # Create the table (DSQL basic schema - minimal features)
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
    cur.execute("SELECT id, name, email, department, salary FROM users1 LIMIT 5;")
    sample_data = cur.fetchall()
    
    print(f"{'ID':<4} {'Name':<15} {'Email':<25} {'Department':<12} {'Salary':<10}")
    print("-" * 70)
    for row in sample_data:
        print(f"{row[0]:<4} {row[1]:<15} {row[2]:<25} {row[3]:<12} ${row[4]:<9}")
    
    print(f"\n[OK] users1 table setup complete!")


def setup_complex_database(cur):
    """Set up users and orders tables for complex queries."""
    print("\n[SETUP] Setting up users and orders tables for complex queries...")
    
    # Create users table
    print("[CREATE] Creating users table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            department VARCHAR(50),
            role VARCHAR(50),
            last_login TIMESTAMP
        );
    """)
    print("[OK] users table created")
    
    # Create orders table
    print("[CREATE] Creating orders table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            order_date DATE NOT NULL,
            order_amount DECIMAL(10,2) NOT NULL,
            order_type VARCHAR(50)
        );
    """)
    print("[OK] orders table created")
    
    # Check if data already exists
    cur.execute("SELECT COUNT(*) FROM users;")
    user_count = cur.fetchone()[0]
    
    if user_count > 0:
        print(f"[EXISTS] users table already has {user_count} rows")
        cur.execute("SELECT COUNT(*) FROM orders;")
        order_count = cur.fetchone()[0]
        print(f"[EXISTS] orders table already has {order_count} rows")
        return
    
    # Insert test data into users
    print("[INSERT] Inserting test data into users table...")
    cur.execute("""
        INSERT INTO users (user_id, name, email, department, role, last_login) VALUES
        (1, 'John Doe', 'john.doe@company.com', 'Engineering', 'Senior Developer', '2024-01-15 10:30:00'),
        (2, 'Jane Smith', 'jane.smith@company.com', 'Marketing', 'Marketing Manager', '2024-01-14 09:15:00'),
        (3, 'Mike Johnson', 'mike.johnson@company.com', 'Engineering', 'Tech Lead', '2024-01-15 11:45:00'),
        (4, 'Sarah Wilson', 'sarah.wilson@company.com', 'Sales', 'Sales Director', '2024-01-13 14:20:00'),
        (5, 'David Brown', 'david.brown@company.com', 'Engineering', 'Developer', '2024-01-15 08:00:00')
        ON CONFLICT (email) DO NOTHING;
    """)
    
    # Insert test data into orders
    print("[INSERT] Inserting test data into orders table...")
    cur.execute("""
        INSERT INTO orders (order_id, user_id, order_date, order_amount, order_type) VALUES
        (1, 1, CURRENT_DATE - INTERVAL '5 days', 150.00, 'Product'),
        (2, 1, CURRENT_DATE - INTERVAL '10 days', 200.00, 'Service'),
        (3, 1, CURRENT_DATE - INTERVAL '15 days', 75.00, 'Product'),
        (4, 2, CURRENT_DATE - INTERVAL '3 days', 300.00, 'Service'),
        (5, 2, CURRENT_DATE - INTERVAL '20 days', 125.00, 'Product'),
        (6, 3, CURRENT_DATE - INTERVAL '7 days', 450.00, 'Product'),
        (7, 3, CURRENT_DATE - INTERVAL '12 days', 180.00, 'Service'),
        (8, 4, CURRENT_DATE - INTERVAL '2 days', 220.00, 'Product'),
        (9, 5, CURRENT_DATE - INTERVAL '8 days', 95.00, 'Service'),
        (10, 5, CURRENT_DATE - INTERVAL '25 days', 310.00, 'Product');
    """)
    
    # Verify data
    cur.execute("SELECT COUNT(*) FROM users;")
    user_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM orders;")
    order_count = cur.fetchone()[0]
    
    print(f"[OK] Test data inserted successfully")
    print(f"[COUNT] Users: {user_count}, Orders: {order_count}")
    
    # Show sample data
    print("\n[SAMPLE] Sample data from users table:")
    cur.execute("SELECT user_id, name, email, department FROM users LIMIT 3;")
    sample_data = cur.fetchall()
    for row in sample_data:
        print(f"   ID: {row[0]}, Name: {row[1]}, Email: {row[2]}, Dept: {row[3]}")
    
    print("\n[SAMPLE] Sample data from orders table:")
    cur.execute("SELECT order_id, user_id, order_date, order_amount, order_type FROM orders LIMIT 3;")
    sample_data = cur.fetchall()
    for row in sample_data:
        print(f"   Order: {row[0]}, User: {row[1]}, Date: {row[2]}, Amount: ${row[3]}, Type: {row[4]}")
    
    print(f"\n[OK] users and orders tables setup complete!")


def setup_database(cluster_endpoint: str, query_type: str = 'simple'):
    """Set up the database schema and test data."""
    conn = create_dsql_connection(cluster_endpoint)
    cur = conn.cursor()
    
    try:
        if query_type == 'simple':
            setup_simple_database(cur)
        elif query_type == 'complex':
            setup_complex_database(cur)
        else:
            print(f"[ERROR] Unknown query type: {query_type}")
            sys.exit(1)
        
    except Exception as e:
        print(f"[ERROR] Error setting up database: {e}")
        raise
    finally:
        conn.close()
        print("[CLOSE] Database connection closed")

def test_query(cluster_endpoint: str):
    """Test the query that will be used in the performance test."""
    print("\n[TEST] Testing the query that will be used in performance test...")
    
    conn = create_dsql_connection(cluster_endpoint)
    cur = conn.cursor()
    
    try:
        start_time = datetime.datetime.now()
        cur.execute("SELECT * FROM users1;")
        results = cur.fetchall()
        end_time = datetime.datetime.now()
        
        query_time = end_time - start_time
        
        print(f"[OK] Query executed successfully!")
        print(f"[COUNT] Returned {len(results)} rows")
        print(f"[TIME] Query time: {query_time}")
        print(f"[READY] Ready for ElastiCache performance comparison!")
        
    except Exception as e:
        print(f"[ERROR] Error testing query: {e}")
        raise
    finally:
        conn.close()

def main():
    # Default DSQL endpoint
    default_dsql_endpoint = "d4abulc3ivg4d4knvmfotcybse.dsql.us-east-1.on.aws"
    
    # Allow override via command line or environment
    cluster_endpoint = os.environ.get('DSQL_ENDPOINT', default_dsql_endpoint)
    query_type = 'simple'  # default
    
    if len(sys.argv) > 1:
        cluster_endpoint = sys.argv[1]
    if len(sys.argv) > 2:
        query_type = sys.argv[2]
    
    print("[START] DSQL Database Setup")
    print("=" * 50)
    print(f"DSQL Endpoint: {cluster_endpoint}")
    print(f"Query Type: {query_type}")
    print(f"Timestamp: {datetime.datetime.now()}")
    print()
    
    try:
        # Setup database
        setup_database(cluster_endpoint, query_type)
        
        # Test the query
        test_query(cluster_endpoint)
        
        print("\n[COMPLETE] Setup complete! You can now run the ElastiCache performance test.")
        
    except Exception as e:
        print(f"\n[ERROR] Setup failed: {e}")
        print("\n[TROUBLESHOOT] Troubleshooting tips:")
        print("   - Ensure you have DSQL permissions")
        print("   - Verify the cluster endpoint is correct")
        print("   - Check that you're running in the correct AWS region")
        sys.exit(1)

if __name__ == "__main__":
    main()
