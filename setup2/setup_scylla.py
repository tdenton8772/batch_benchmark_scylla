#!/usr/bin/env python3
"""
Setup Scylla database: create keyspace, table, and load CSV data.
"""

import argparse
import csv
import glob
import os
import sys
import uuid
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from dotenv import load_dotenv

# Load environment variables from script directory
script_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(script_dir, '.env'))


def create_keyspace_and_table(session, keyspace_name: str, replication_factor: int = 3):
    """Create keyspace and table."""
    
    # Create keyspace
    print(f"Creating keyspace: {keyspace_name}")
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
        WITH REPLICATION = {{
            'class': 'SimpleStrategy',
            'replication_factor': {replication_factor}
        }}
    """)
    
    # Use the keyspace
    session.set_keyspace(keyspace_name)
    
    # Create table
    print("Creating table: content_data")
    session.execute("""
        CREATE TABLE IF NOT EXISTS content_data (
            sort_key UUID PRIMARY KEY,
            content_embedding_chunks TEXT,
            content_text_chunks TEXT,
            content_title TEXT,
            content_url_domain TEXT,
            content_url_origin TEXT,
            expire_at BIGINT,
            metadata_author TEXT,
            metadata_content_updated_ts TEXT,
            metadata_crawled_ts TEXT,
            metadata_description TEXT,
            metadata_hostname TEXT,
            metadata_published_date TEXT,
            metadata_sitename TEXT,
            metadata_updated_ts TEXT,
            scrape_info_consumer_name TEXT,
            scrape_info_teapot_normalized_url TEXT,
            system_info_content_type TEXT,
            updated_at BIGINT
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND comment = ''
    AND compaction = {'class': 'IncrementalCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '16', 'sstable_compression': 'ZstdWithDictsCompressor'}
    AND crc_check_chance = 1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND speculative_retry = '99.0PERCENTILE'
    AND tombstone_gc = {'mode': 'repair', 'propagation_delay_in_seconds': '3600'};

        """)
    
    print("Keyspace and table created successfully!")


def load_csv_data(session, keyspace_name: str, csv_file: str, batch_size: int = 50):
    """Load data from CSV file into the table."""
    
    session.set_keyspace(keyspace_name)
    
    # Prepare the insert statement
    insert_stmt = session.prepare("""
        INSERT INTO content_data (
            sort_key, content_embedding_chunks, content_text_chunks,
            content_title, content_url_domain, content_url_origin,
            expire_at, metadata_author, metadata_content_updated_ts,
            metadata_crawled_ts, metadata_description, metadata_hostname,
            metadata_published_date, metadata_sitename, metadata_updated_ts,
            scrape_info_consumer_name, scrape_info_teapot_normalized_url,
            system_info_content_type, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    
    print(f"Loading data from {csv_file}...")
    
    rows_loaded = 0
    batch = []
    
    # Set CSV field size limit to handle large fields
    csv.field_size_limit(sys.maxsize)
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            try:
                # Parse the row
                batch.append({
                    'sort_key': uuid.UUID(row['sort_key']),
                    'content_embedding_chunks': row['content_embedding_chunks'],
                    'content_text_chunks': row['content_text_chunks'],
                    'content_title': row['content_title'],
                    'content_url_domain': row['content_url_domain'],
                    'content_url_origin': row['content_url_origin'],
                    'expire_at': int(row['expire_at']),
                    'metadata_author': row['metadata_author'],
                    'metadata_content_updated_ts': row['metadata_content_updated_ts'],
                    'metadata_crawled_ts': row['metadata_crawled_ts'],
                    'metadata_description': row['metadata_description'],
                    'metadata_hostname': row['metadata_hostname'],
                    'metadata_published_date': row['metadata_published_date'],
                    'metadata_sitename': row['metadata_sitename'],
                    'metadata_updated_ts': row['metadata_updated_ts'],
                    'scrape_info_consumer_name': row['scrape_info_consumer_name'],
                    'scrape_info_teapot_normalized_url': row['scrape_info_teapot_normalized_url'],
                    'system_info_content_type': row['system_info_content_type'],
                    'updated_at': int(row['updated_at'])
                })
                
                # Execute batch when full
                if len(batch) >= batch_size:
                    for batch_row in batch:
                        session.execute(insert_stmt, batch_row)
                    rows_loaded += len(batch)
                    print(f"  Loaded {rows_loaded} rows...", end='\r')
                    batch = []
                    
            except Exception as e:
                print(f"\nError processing row in {csv_file}: {e}")
                continue
        
        # Execute remaining batch
        if batch:
            for batch_row in batch:
                session.execute(insert_stmt, batch_row)
            rows_loaded += len(batch)
    
    print(f"\n  Completed loading {csv_file}: {rows_loaded} rows loaded")
    return rows_loaded


def main():
    parser = argparse.ArgumentParser(
        description='Setup Scylla keyspace, table, and load CSV data'
    )
    parser.add_argument(
        '--hosts',
        nargs='+',
        default=os.getenv('SCYLLA_HOST', '127.0.0.1').split(',') if os.getenv('SCYLLA_HOST') else ['127.0.0.1'],
        help='Scylla host addresses (default: from .env or 127.0.0.1)'
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
        '--replication-factor',
        type=int,
        default=3,
        help='Replication factor (default: 3)'
    )
    parser.add_argument(
        '--username',
        default=os.getenv('SCYLLA_USERNAME', 'scylla'),
        help='Username for authentication (default: from .env or scylla)'
    )
    parser.add_argument(
        '--password',
        default=os.getenv('SCYLLA_PASSWORD', ''),
        help='Password for authentication (default: from .env or empty)'
    )
    parser.add_argument(
        '--skip-create',
        action='store_true',
        help='Skip keyspace and table creation (only load data)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=50,
        help='Batch size for loading data (default: 50)'
    )
    parser.add_argument(
        '--csv-pattern',
        default='data_*.csv',
        help='Pattern to match CSV files (default: data_*.csv)'
    )
    
    args = parser.parse_args()
    
    # Connect to Scylla cluster
    print(f"Connecting to Scylla cluster at {args.hosts}...")
    
    if args.username and args.password:
        auth_provider = PlainTextAuthProvider(username=args.username, password=args.password)
        cluster = Cluster(args.hosts, port=args.port, auth_provider=auth_provider)
    else:
        cluster = Cluster(args.hosts, port=args.port)
    
    session = cluster.connect()
    print("Connected successfully!")
    
    try:
        # Create keyspace and table if not skipping
        if not args.skip_create:
            create_keyspace_and_table(session, args.keyspace, args.replication_factor)
        
        # Find CSV files (support recursive patterns with **)
        csv_files = sorted(glob.glob(args.csv_pattern, recursive=True))
        
        if not csv_files:
            print(f"No CSV files found matching pattern: {args.csv_pattern}")
            return
        
        print(f"\nFound {len(csv_files)} CSV file(s) to load")
        
        # Load each CSV file
        total_rows = 0
        for csv_file in csv_files:
            rows_loaded = load_csv_data(session, args.keyspace, csv_file, args.batch_size)
            total_rows += rows_loaded
        
        print(f"\n{'='*60}")
        print(f"Successfully loaded {total_rows} total rows from {len(csv_files)} file(s)")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        session.shutdown()
        cluster.shutdown()
        print("\nConnection closed.")


if __name__ == '__main__':
    main()


