#!/usr/bin/env python3
"""
Extract primary keys from ScyllaDB table and write to CSV files.

This script scans an entire ScyllaDB table and extracts only the primary keys
(sort_key column) into CSV files for use with the query load testing framework.
"""

import argparse
import csv
import os
import sys
import time
from typing import List
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def extract_keys_to_csv(
    session,
    keyspace: str,
    table: str,
    key_column: str,
    output_dir: str,
    rows_per_file: int,
    page_size: int = 5000
):
    """
    Extract primary keys from table and write to CSV files.
    
    Args:
        session: Cassandra session
        keyspace: Keyspace name
        table: Table name
        key_column: Name of the key column to extract
        output_dir: Output directory for CSV files
        rows_per_file: Number of rows per CSV file
        page_size: Fetch page size for query
    """
    session.set_keyspace(keyspace)
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Query to select only the key column
    query = f"SELECT {key_column} FROM {table}"
    statement = SimpleStatement(query, fetch_size=page_size)
    
    print(f"Extracting keys from {keyspace}.{table}...")
    print(f"Output directory: {output_dir}")
    print(f"Rows per file: {rows_per_file}")
    print()
    
    start_time = time.time()
    
    # Execute query
    rows = session.execute(statement)
    
    file_num = 1
    row_count = 0
    total_rows = 0
    current_file = None
    writer = None
    
    try:
        for row in rows:
            # Open new file if needed
            if row_count == 0:
                if current_file:
                    current_file.close()
                
                filename = os.path.join(output_dir, f"keys_{file_num:04d}.csv")
                current_file = open(filename, 'w', newline='', encoding='utf-8')
                writer = csv.writer(current_file)
                
                # Write header
                writer.writerow([key_column])
                
                print(f"Writing file {file_num}: {filename}")
                file_num += 1
            
            # Write key
            writer.writerow([str(row[0])])
            row_count += 1
            total_rows += 1
            
            # Progress indicator
            if total_rows % 100000 == 0:
                elapsed = time.time() - start_time
                rate = total_rows / elapsed
                print(f"  Processed {total_rows:,} rows ({rate:.0f} rows/sec)")
            
            # Check if file is full
            if row_count >= rows_per_file:
                row_count = 0
        
        # Close last file
        if current_file:
            current_file.close()
    
    except Exception as e:
        print(f"\nError during extraction: {e}")
        if current_file:
            current_file.close()
        raise
    
    elapsed = time.time() - start_time
    print()
    print("="*80)
    print(f"Extraction complete!")
    print(f"Total rows: {total_rows:,}")
    print(f"Total files: {file_num - 1}")
    print(f"Time elapsed: {elapsed:.1f}s")
    print(f"Throughput: {total_rows/elapsed:.0f} rows/sec")
    print("="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Extract primary keys from ScyllaDB table to CSV files'
    )
    parser.add_argument(
        '--hosts',
        nargs='+',
        default=os.getenv('SCYLLA_HOSTS', '127.0.0.1').split(',') if os.getenv('SCYLLA_HOSTS') else ['127.0.0.1'],
        help='ScyllaDB host addresses (default: from .env or 127.0.0.1)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=int(os.getenv('SCYLLA_PORT', '9042')),
        help='Port number (default: from .env or 9042)'
    )
    parser.add_argument(
        '--keyspace',
        default=os.getenv('SCYLLA_KEYSPACE', 'content_db'),
        help='Keyspace name (default: from .env or content_db)'
    )
    parser.add_argument(
        '--table',
        default=os.getenv('SCYLLA_TABLE', 'content_data'),
        help='Table name (default: from .env or content_data)'
    )
    parser.add_argument(
        '--key-column',
        default='sort_key',
        help='Name of the key column to extract (default: sort_key)'
    )
    parser.add_argument(
        '--username',
        default=os.getenv('SCYLLA_USERNAME', ''),
        help='Username for authentication (default: from .env or empty)'
    )
    parser.add_argument(
        '--password',
        default=os.getenv('SCYLLA_PASSWORD', ''),
        help='Password for authentication (default: from .env or empty)'
    )
    parser.add_argument(
        '--output-dir',
        default='extracted_keys',
        help='Output directory for CSV files (default: extracted_keys)'
    )
    parser.add_argument(
        '--rows-per-file',
        type=int,
        default=50000,
        help='Number of rows per CSV file (default: 50000)'
    )
    parser.add_argument(
        '--page-size',
        type=int,
        default=5000,
        help='Fetch page size for query (default: 5000)'
    )
    
    args = parser.parse_args()
    
    # Connect to ScyllaDB cluster
    print(f"Connecting to ScyllaDB cluster at {args.hosts}...")
    
    try:
        if args.username and args.password:
            auth_provider = PlainTextAuthProvider(username=args.username, password=args.password)
            cluster = Cluster(args.hosts, port=args.port, auth_provider=auth_provider)
        else:
            cluster = Cluster(args.hosts, port=args.port)
        
        session = cluster.connect()
        print("Connected successfully!")
        print()
        
        # Extract keys
        extract_keys_to_csv(
            session,
            args.keyspace,
            args.table,
            args.key_column,
            args.output_dir,
            args.rows_per_file,
            args.page_size
        )
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    finally:
        if 'session' in locals():
            session.shutdown()
        if 'cluster' in locals():
            cluster.shutdown()
        print("\nConnection closed.")


if __name__ == '__main__':
    main()
