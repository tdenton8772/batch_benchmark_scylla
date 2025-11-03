#!/usr/bin/env python3
"""
Query Worker: Multi-threaded worker for streaming CSV files and querying ScyllaDB.

Architecture:
- Reader Thread: Reads CSV files, extracts sort_key values, batches them into queue
- Query Thread: Consumes batches from queue, issues concurrent queries to ScyllaDB
"""

import argparse
import csv
import logging
import os
import queue
import random
import signal
import sys
import threading
import time
import uuid
import requests
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict, Any

from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy, FallthroughRetryPolicy, ConstantSpeculativeExecutionPolicy, WhiteListRoundRobinPolicy, RackAwareRoundRobinPolicy
from cassandra.query import SimpleStatement
from dotenv import load_dotenv
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra import ProtocolVersion

# Load environment variables
load_dotenv()

# Sentinel value to signal end of data
SENTINEL = None


def get_ec2_metadata(path: str, timeout: float = 0.1) -> Optional[str]:
    """Fetch EC2 instance metadata with short timeout."""
    try:
        # IMDSv2 - get token first
        token_url = 'http://169.254.169.254/latest/api/token'
        token_headers = {'X-aws-ec2-metadata-token-ttl-seconds': '21600'}
        token_response = requests.put(token_url, headers=token_headers, timeout=timeout)
        token = token_response.text
        
        # Fetch metadata with token
        metadata_url = f'http://169.254.169.254/latest/meta-data/{path}'
        headers = {'X-aws-ec2-metadata-token': token}
        response = requests.get(metadata_url, headers=headers, timeout=timeout)
        
        if response.status_code == 200:
            return response.text.strip()
    except Exception:
        pass
    return None


def detect_datacenter_and_rack() -> tuple[Optional[str], Optional[str]]:
    """Auto-detect datacenter and rack from cloud provider metadata.
    
    Returns:
        (datacenter, rack) tuple. Both can be None if detection fails.
    """
    # Try AWS EC2
    availability_zone = get_ec2_metadata('placement/availability-zone')
    if availability_zone:
        # Extract region from AZ (e.g., us-east-1a -> us-east-1)
        region = availability_zone[:-1]
        az_suffix = availability_zone[-1]  # 'a', 'b', etc.
        
        # ScyllaDB typically uses rack names like "use1-az1", "use1-az2", etc.
        # Map AZ letter to number (a=1, b=2, c=3, d=4, e=5, f=6)
        az_num = str(ord(az_suffix) - ord('a') + 1)
        
        # Format: use1-az1, usw2-az3, etc.
        region_abbr = region.replace('us-east-', 'use').replace('us-west-', 'usw').replace('eu-west-', 'euw').replace('-', '')
        rack = f"{region_abbr}-az{az_num}"
        
        # Datacenter: ScyllaDB AWS convention is uppercase with underscores
        # e.g., us-east-1 -> AWS_US_EAST_1
        datacenter = f"AWS_{region.upper().replace('-', '_')}"
        
        return (datacenter, rack)
    
    # Could add GCP/Azure detection here
    
    return (None, None)


class WorkerMetrics:
    """Thread-safe metrics tracker."""
    
    def __init__(self):
        self.lock = threading.Lock()
        self.total_submitted = 0
        self.total_ok = 0
        self.total_found = 0
        self.total_not_found = 0
        self.total_timeouts = 0
        self.total_errors = 0
        self.files_processed = 0
        self.rows_read = 0
        self.recent_queries = deque(maxlen=50000)  # Store timestamps for QPS calculation
    
    def record_batch_submitted(self, count: int):
        with self.lock:
            self.total_submitted += count
    
    def record_batch_results(self, ok: int, found: int, not_found: int, timeouts: int, errors: int):
        with self.lock:
            self.total_ok += ok
            self.total_found += found
            self.total_not_found += not_found
            self.total_timeouts += timeouts
            self.total_errors += errors
            now = time.time()
            for _ in range(ok):
                self.recent_queries.append(now)
    
    def record_rows_read(self, count: int):
        with self.lock:
            self.rows_read += count
    
    def record_file_completed(self):
        with self.lock:
            self.files_processed += 1
    
    def get_qps(self, window_secs: float = 5.0) -> float:
        """Calculate queries per second over the last N seconds."""
        with self.lock:
            if not self.recent_queries:
                return 0.0
            now = time.time()
            cutoff = now - window_secs
            recent = [t for t in self.recent_queries if t >= cutoff]
            return len(recent) / window_secs if recent else 0.0
    
    def get_snapshot(self) -> Dict[str, Any]:
        """Get current metrics snapshot."""
        with self.lock:
            return {
                'submitted': self.total_submitted,
                'ok': self.total_ok,
                'found': self.total_found,
                'not_found': self.total_not_found,
                'timeouts': self.total_timeouts,
                'errors': self.total_errors,
                'files_processed': self.files_processed,
                'rows_read': self.rows_read,
            }


def setup_logging(worker_id: int) -> logging.Logger:
    """Set up logging with worker context."""
    logger = logging.getLogger(f'worker-{worker_id}')
    logger.setLevel(logging.INFO)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    formatter = logging.Formatter(
        f'%(asctime)s | worker={worker_id} | thread=%(threadName)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S'
    )
    
    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)
    
    # File handler - always write to file so we can debug even if stdout captured
    try:
        log_file = f'/tmp/query_worker_{worker_id}.log'
        file_handler = logging.FileHandler(log_file, mode='a')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        print(f"Logging to {log_file}", file=sys.stderr)
    except Exception as e:
        print(f"WARNING: Could not create log file: {e}", file=sys.stderr)
    
    return logger


def reader_thread(
    files: List[str],
    batch_queue: queue.Queue,
    metrics: WorkerMetrics,
    stop_event: threading.Event,
    config: Dict[str, Any],
    logger: logging.Logger
):
    """
    Reader thread: reads CSV files, extracts sort_key values, batches and queues them.
    """
    threading.current_thread().name = 'reader'
    logger.info(f"Reader thread started with {len(files)} file(s)")
    
    batch_size = config['batch_size']
    sort_key_column = config['sort_key_column']
    csv_has_header = config['csv_has_header']
    
    current_batch = []
    
    try:
        for file_path in files:
            if stop_event.is_set():
                logger.info(f"Stop event set, reader exiting early")
                break
            
            logger.info(f"Processing file: {file_path}")
            file_rows = 0
            
            try:
                with open(file_path, 'r', newline='', encoding='utf-8') as f:
                    # Set CSV field size limit for large fields
                    csv.field_size_limit(sys.maxsize)
                    
                    if csv_has_header:
                        reader = csv.DictReader(f)
                        key_col = sort_key_column
                    else:
                        # If no header, assume sort_key is first column (index 0)
                        reader = csv.reader(f)
                        key_col = 0
                    
                    for row in reader:
                        if stop_event.is_set():
                            break
                        
                        try:
                            # Extract sort_key
                            if csv_has_header:
                                sort_key_str = row.get(key_col, '').strip()
                            else:
                                sort_key_str = row[key_col].strip() if len(row) > key_col else ''
                            
                            if not sort_key_str:
                                continue
                            
                            # Parse UUID
                            sort_key = uuid.UUID(sort_key_str)
                            current_batch.append(sort_key)
                            file_rows += 1
                            
                            # When batch is full, queue it
                            if len(current_batch) >= batch_size:
                                batch_queue.put(current_batch, block=True)
                                metrics.record_rows_read(len(current_batch))
                                current_batch = []
                        
                        except (ValueError, IndexError) as e:
                            logger.warning(f"Malformed row in {file_path}: {e}")
                            continue
                
                logger.info(f"Completed file: {file_path} ({file_rows} rows)")
                metrics.record_file_completed()
            
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")
                continue
        
        # Queue remaining batch if any
        if current_batch and not stop_event.is_set():
            batch_queue.put(current_batch, block=True)
            metrics.record_rows_read(len(current_batch))
        
        # Send sentinel to signal completion
        batch_queue.put(SENTINEL)
        logger.info("Reader thread completed, sentinel sent")
    
    except Exception as e:
        logger.error(f"Reader thread fatal error: {e}", exc_info=True)
        batch_queue.put(SENTINEL)


def query_thread(
    batch_queue: queue.Queue,
    metrics: WorkerMetrics,
    stop_event: threading.Event,
    config: Dict[str, Any],
    logger: logging.Logger
):
    """
    Query thread: consumes batches from queue, issues concurrent queries to ScyllaDB.
    """
    threading.current_thread().name = 'query'
    logger.info("Query thread started")
    
    cluster = None
    session = None
    
    try:
        # Connect to ScyllaDB
        if config['dry_run']:
            logger.info("DRY RUN mode - no database connection")
        else:
            logger.info(f"Connecting to ScyllaDB at {config['scylla_hosts']}")
            
            contact_points = config['scylla_hosts'].split(',')
            port = config['scylla_port']
            
            # Auth provider if credentials provided
            auth_provider = None
            if config['scylla_username'] and config['scylla_password']:
                auth_provider = PlainTextAuthProvider(
                    username=config['scylla_username'],
                    password=config['scylla_password']
                )
            
            # Consistency level
            consistency_map = {
                'ONE': ConsistencyLevel.ONE,
                'LOCAL_ONE': ConsistencyLevel.LOCAL_ONE,
                'QUORUM': ConsistencyLevel.QUORUM,
                'LOCAL_QUORUM': ConsistencyLevel.LOCAL_QUORUM,
                'ALL': ConsistencyLevel.ALL,
            }
            consistency = consistency_map.get(
                config['scylla_consistency'],
                ConsistencyLevel.LOCAL_ONE
            )
            
            # Auto-detect or use configured DC/rack
            detected_dc, detected_rack = detect_datacenter_and_rack()
            
            # Use detected values, fall back to config, then defaults
            local_dc = detected_dc or config.get('local_dc') or 'AWS_US_EAST_1'
            local_rack = detected_rack or config.get('local_rack')
            
            logger.info(f"Datacenter: {local_dc} (detected={detected_dc is not None}, source={'detected' if detected_dc else 'config/default'})")
            logger.info(f"Rack: {local_rack} (detected={detected_rack is not None}, source={'detected' if detected_rack else 'config/default'})")
            logger.warning(f"IMPORTANT: DC name must match ScyllaDB cluster datacenter. Detected: {local_dc}")
            
            # Production-grade execution profile with TokenAware + RackAware
            # RackAwareRoundRobinPolicy prefers local rack nodes, then local DC nodes, then remote DCs
            if local_rack:
                base_policy = RackAwareRoundRobinPolicy(local_dc=local_dc, local_rack=local_rack)
                logger.info(f"Using RackAwareRoundRobinPolicy: dc={local_dc}, rack={local_rack}")
            else:
                base_policy = DCAwareRoundRobinPolicy(local_dc=local_dc)
                logger.info(f"Using DCAwareRoundRobinPolicy: dc={local_dc}")
            
            lbp = TokenAwarePolicy(base_policy, shuffle_replicas=True)
            profile = ExecutionProfile(
                load_balancing_policy=lbp,
                consistency_level=consistency,
                request_timeout=config['query_timeout_secs'],
                # speculative_execution_policy=ConstantSpeculativeExecutionPolicy(
                #     delay=config['query_timeout_secs'] * 0.10,  # 10% of timeout
                #     max_attempts=2
                # ),
                retry_policy=FallthroughRetryPolicy(),
            )
            
            # Create cluster with increased connection pool for high concurrency
            # Set max_requests_per_connection to support higher concurrency per connection
            # Note: Use libev or gevent event loop for Python 3.12+
            event_loop_used = None
            try:
                from cassandra.io.libevreactor import LibevConnection
                LibevConnection.max_in_flight = 32768
                event_loop_used = 'libev'
                logger.info(f"Using LibevConnection with max_in_flight=32768")
            except ImportError:
                try:
                    from cassandra.io.geventreactor import GeventConnection
                    GeventConnection.max_in_flight = 32768
                    event_loop_used = 'gevent'
                    logger.info(f"Using GeventConnection with max_in_flight=32768")
                except ImportError:
                    # Fallback - driver will use default event loop
                    event_loop_used = 'default'
                    logger.warning("Using default event loop - may have lower throughput")

            cluster = Cluster(
                contact_points=contact_points,
                port=port,
                protocol_version=4,
                auth_provider=auth_provider,
                execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                connect_timeout=60,
                control_connection_timeout=60,
                idle_heartbeat_interval=30,  # Keep connections alive
                idle_heartbeat_timeout=30,
                prepare_on_all_hosts=True,
                compression='lz4',
                reprepare_on_up=True,
            )
            
            # Retry connection up to 3 times with exponential backoff
            max_retries = 3
            retry_delay = 2
            for attempt in range(1, max_retries + 1):
                try:
                    session = cluster.connect()
                    session.set_keyspace(config['scylla_keyspace'])
                    break  # Success!
                except Exception as e:
                    if attempt == max_retries:
                        logger.error(f"Failed to connect after {max_retries} attempts: {e}")
                        raise
                    logger.warning(f"Connection attempt {attempt}/{max_retries} failed: {e}, retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
            
            # Prepare statement with idempotency and fetch_size
            cql = f"SELECT * FROM {config['scylla_table']} WHERE sort_key = ? LIMIT 1 BYPASS CACHE"
            prepared = session.prepare(cql)
            prepared.is_idempotent = True
            prepared.consistency_level = consistency
            prepared.fetch_size = 1
            
            logger.info(f"Connected to ScyllaDB, keyspace={config['scylla_keyspace']}")
        
        # Metrics reporting
        last_metrics_time = time.time()
        metrics_interval = config['metrics_interval_secs']
        
        # Process batches
        while not stop_event.is_set():
            try:
                # Get batch from queue (timeout to allow periodic metrics logging)
                try:
                    batch = batch_queue.get(timeout=1.0)
                except queue.Empty:
                    batch = batch_queue.get(timeout=1.0)
                except queue.Empty:
                    # Log metrics periodically even if no new batches
                    now = time.time()
                    if now - last_metrics_time >= metrics_interval:
                        log_metrics(logger, metrics, batch_queue)
                        last_metrics_time = now
                    continue
                
                # Check for sentinel
                if batch is SENTINEL:
                    logger.info("Received sentinel, query thread shutting down")
                    break
                
                batch_size = len(batch)
                metrics.record_batch_submitted(batch_size)
                
                # Execute queries
                if config['dry_run']:
                    # Dry run: simulate query execution
                    time.sleep(0.001 * batch_size)  # Simulate some work
                    metrics.record_batch_results(ok=batch_size, found=batch_size, not_found=0, timeouts=0, errors=0)
                else:
                    # Execute concurrent queries with ThreadPoolExecutor
                    concurrency = config['concurrency']
                    jitter_ms = config.get('jitter_ms', 0)
                    
                    batch_start = time.time()
                    try:
                        # Execute queries with jitter using ThreadPoolExecutor
                        def execute_single_query(key):
                            # Add jitter before executing
                            if jitter_ms > 0:
                                time.sleep(random.uniform(0, jitter_ms / 1000.0))
                            
                            try:
                                result = session.execute(prepared, (key,))
                                row = result.one()
                                return (True, row)
                            except Exception as e:
                                return (False, e)
                        
                        # Submit all queries to thread pool
                        results = []
                        with ThreadPoolExecutor(max_workers=concurrency) as executor:
                            futures = {executor.submit(execute_single_query, key): key for key in batch}
                            
                            for future in as_completed(futures):
                                try:
                                    success, result = future.result()
                                    results.append((success, result))
                                except Exception as e:
                                    results.append((False, e))
                        
                        # Count results: found, not_found, timeouts, errors
                        ok_count = 0
                        found_count = 0
                        not_found_count = 0
                        timeout_count = 0
                        error_count = 0
                        
                        for success, result in results:
                            if success:
                                # Query succeeded - check if row was found
                                ok_count += 1
                                row = result.one()
                                if row is None:
                                    not_found_count += 1
                                else:
                                    found_count += 1
                            else:
                                # Query failed - categorize error
                                error_type = type(result).__name__
                                if 'Timeout' in error_type or 'TimedOut' in error_type:
                                    timeout_count += 1
                                    logger.warning(f"Query timeout: {error_type}")
                                else:
                                    error_count += 1
                                    logger.warning(f"Query error: {error_type} - {result}")
                        
                        batch_duration = time.time() - batch_start
                        per_req_duration = batch_duration / max(1, len(args_list))
                        
                        logger.debug(
                            f"Batch: {len(args_list)} keys - found={found_count}, not_found={not_found_count}, "
                            f"timeouts={timeout_count}, errors={error_count}, concurrency={concurrency}, "
                            f"elapsed={batch_duration:.3f}s ({per_req_duration:.6f}s/req)"
                        )
                        
                        metrics.record_batch_results(
                            ok=ok_count,
                            found=found_count,
                            not_found=not_found_count,
                            timeouts=timeout_count,
                            errors=error_count
                        )
                    
                    except Exception as e:
                        logger.error(f"Batch execution error: {e}")
                        metrics.record_batch_results(ok=0, found=0, not_found=0, timeouts=0, errors=batch_size)
                
                # Log metrics periodically
                now = time.time()
                if now - last_metrics_time >= metrics_interval:
                    log_metrics(logger, metrics, batch_queue)
                    last_metrics_time = now
                
                batch_queue.task_done()
            
            except Exception as e:
                logger.error(f"Query thread error: {e}", exc_info=True)
        
        # Final metrics
        log_metrics(logger, metrics, batch_queue)
        logger.info("Query thread completed")
    
    except Exception as e:
        logger.error(f"Query thread fatal error: {e}", exc_info=True)
    
    finally:
        if session:
            session.shutdown()
        if cluster:
            cluster.shutdown()
        logger.info("Database connection closed")


def log_metrics(logger: logging.Logger, metrics: WorkerMetrics, batch_queue: queue.Queue):
    """Log current metrics."""
    snapshot = metrics.get_snapshot()
    qps = metrics.get_qps()
    queue_depth = batch_queue.qsize()
    
    logger.info(
        f"qps={qps:.1f} | "
        f"ok={snapshot['ok']} | "
        f"found={snapshot['found']} | "
        f"not_found={snapshot['not_found']} | "
        f"timeouts={snapshot['timeouts']} | "
        f"errors={snapshot['errors']} | "
        f"submitted={snapshot['submitted']} | "
        f"rows_read={snapshot['rows_read']} | "
        f"files={snapshot['files_processed']} | "
        f"queue_depth={queue_depth}"
    )


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Query worker: stream CSV files and query ScyllaDB'
    )
    
    # Worker identification
    parser.add_argument('--worker-id', type=int, required=True, help='Worker ID')
    parser.add_argument('--files', nargs='+', help='CSV files to process')
    
    # CSV configuration
    parser.add_argument('--csv-has-header', type=lambda x: x.lower() == 'true',
                        default=os.getenv('CSV_HAS_HEADER', 'true').lower() == 'true',
                        help='CSV has header row')
    parser.add_argument('--sort-key-column', default=os.getenv('SORT_KEY_COLUMN', 'sort_key'),
                        help='Name of sort_key column')
    
    # Worker configuration
    parser.add_argument('--batch-size', type=int, default=int(os.getenv('BATCH_SIZE', '100')),
                        help='Batch size for queries')
    parser.add_argument('--queue-size', type=int, default=int(os.getenv('QUEUE_SIZE', '1000')),
                        help='Maximum queue size')
    parser.add_argument('--concurrency', type=int, default=int(os.getenv('CONCURRENCY', '50')),
                        help='Concurrent queries per batch')
    parser.add_argument('--query-timeout-secs', type=int,
                        default=int(os.getenv('QUERY_TIMEOUT_SECS', '5')),
                        help='Query timeout in seconds')
    parser.add_argument('--metrics-interval-secs', type=int,
                        default=int(os.getenv('METRICS_INTERVAL_SECS', '5')),
                        help='Metrics logging interval in seconds')
    parser.add_argument('--jitter-ms', type=float, default=float(os.getenv('JITTER_MS', '0')),
                        help='Jitter in milliseconds before each query (default: 0)')
    
    # ScyllaDB configuration
    parser.add_argument('--hosts', default=os.getenv('SCYLLA_HOSTS', '127.0.0.1'),
                        help='ScyllaDB hosts (comma-separated)')
    parser.add_argument('--port', type=int, default=int(os.getenv('SCYLLA_PORT', '9042')),
                        help='ScyllaDB port')
    parser.add_argument('--keyspace', default=os.getenv('SCYLLA_KEYSPACE', 'content_db'),
                        help='ScyllaDB keyspace')
    parser.add_argument('--table', default=os.getenv('SCYLLA_TABLE', 'content_data'),
                        help='ScyllaDB table')
    parser.add_argument('--username', default=os.getenv('SCYLLA_USERNAME', ''),
                        help='ScyllaDB username')
    parser.add_argument('--password', default=os.getenv('SCYLLA_PASSWORD', ''),
                        help='ScyllaDB password')
    parser.add_argument('--consistency', default=os.getenv('SCYLLA_CONSISTENCY', 'LOCAL_ONE'),
                        help='Consistency level')
    parser.add_argument('--local-dc', default=os.getenv('LOCAL_DC', None),
                        help='Local datacenter name (auto-detected from AWS metadata if not specified)')
    parser.add_argument('--local-rack', default=os.getenv('LOCAL_RACK', None),
                        help='Local rack name (auto-detected from AWS AZ if not specified)')
    
    # Operational
    parser.add_argument('--dry-run', action='store_true', help='Dry run (no DB queries)')
    
    return parser.parse_args()


def main():
    args = None
    logger = None
    
    try:
        args = parse_args()
        
        # Set up logging
        logger = setup_logging(args.worker_id)
        logger.info(f"Worker {args.worker_id} starting")
        logger.info(f"Python version: {sys.version}")
        logger.info(f"Current directory: {os.getcwd()}")
        
        # Get file list from args or env
        files = args.files
        if not files:
            assigned_files_env = os.getenv('ASSIGNED_FILES', '')
            if assigned_files_env:
                files = [f.strip() for f in assigned_files_env.split(',') if f.strip()]
        
        if not files:
            logger.error("No files assigned to worker")
            sys.exit(1)
        
        logger.info(f"Assigned files: {files}")
        
        # Configuration
        config = {
            'batch_size': args.batch_size,
            'queue_size': args.queue_size,
            'concurrency': args.concurrency,
            'query_timeout_secs': args.query_timeout_secs,
            'metrics_interval_secs': args.metrics_interval_secs,
            'jitter_ms': args.jitter_ms,
            'sort_key_column': args.sort_key_column,
            'csv_has_header': args.csv_has_header,
            'scylla_hosts': args.hosts,
            'scylla_port': args.port,
            'scylla_keyspace': args.keyspace,
            'scylla_table': args.table,
            'scylla_username': args.username,
            'scylla_password': args.password,
            'scylla_consistency': args.consistency,
            'local_dc': args.local_dc,
            'local_rack': args.local_rack,
            'dry_run': args.dry_run,
        }
        
        logger.info(f"Configuration: batch_size={config['batch_size']}, "
                    f"queue_size={config['queue_size']}, concurrency={config['concurrency']}, "
                    f"dry_run={config['dry_run']}")
        
        # Create queue and metrics
        batch_queue = queue.Queue(maxsize=config['queue_size'])
        metrics = WorkerMetrics()
        stop_event = threading.Event()
        
        # Signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown")
            stop_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start threads
        logger.info("Starting reader and query threads...")
        reader = threading.Thread(
            target=reader_thread,
            args=(files, batch_queue, metrics, stop_event, config, logger),
            name='reader'
        )
        
        query = threading.Thread(
            target=query_thread,
            args=(batch_queue, metrics, stop_event, config, logger),
            name='query'
        )
        
        reader.start()
        query.start()
        logger.info("Threads started, waiting for completion...")
        
        # Wait for threads to complete
        reader.join()
        logger.info("Reader thread joined")
        query.join()
        logger.info("Query thread joined")
        
        # Final summary
        snapshot = metrics.get_snapshot()
        logger.info(f"Worker {args.worker_id} completed: "
                    f"rows_read={snapshot['rows_read']}, "
                    f"queries_ok={snapshot['ok']}, "
                    f"timeouts={snapshot['timeouts']}, "
                    f"errors={snapshot['errors']}, "
                    f"files_processed={snapshot['files_processed']}")
    
    except Exception as e:
        error_msg = f"FATAL ERROR in main: {e}"
        if logger:
            logger.error(error_msg, exc_info=True)
        else:
            print(error_msg, file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()


