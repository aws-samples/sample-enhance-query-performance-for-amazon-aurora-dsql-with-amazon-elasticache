#!/usr/bin/env python3
"""
DSQL Database Setup Script
Creates the users1 table and populates it with test data for the ElastiCache demo.
"""

import os
import sys
import logging
import datetime

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

def setup_database(cluster_endpoint: str):
    """Set up the database schema and test data."""
    conn = create_dsql_connection(cluster_endpoint)
    cur = conn.cursor()
    
    try:
        print("\n[SETUP] Setting up database schema and test data...")
        
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
        
        print(f"\n[OK] Database setup complete!")
        print(f"[READY] You can now run the performance test with: select * from users1")
        
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
    if len(sys.argv) > 1:
        cluster_endpoint = sys.argv[1]
    
    print("[START] DSQL Database Setup")
    print("=" * 50)
    print(f"DSQL Endpoint: {cluster_endpoint}")
    print(f"Timestamp: {datetime.datetime.now()}")
    print()
    
    try:
        # Setup database
        setup_database(cluster_endpoint)
        
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
