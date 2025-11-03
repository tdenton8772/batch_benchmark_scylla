#!/usr/bin/env python3
"""
Coordinator: Discovers CSV files and coordinates workers across local or Docker deployments.

Modes:
- local: Spawn worker subprocesses locally
- docker: Generate docker-compose.yml with N worker services
"""

import argparse
import glob
import json
import os
import subprocess
import signal
import sys
import yaml
from pathlib import Path
from typing import List, Dict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def discover_files(csv_dir: str, file_glob: str) -> List[str]:
    """Discover CSV files in directory matching pattern."""
    pattern = os.path.join(csv_dir, file_glob)
    files = sorted(glob.glob(pattern))
    return files


def distribute_files(files: List[str], worker_count: int) -> Dict[int, List[str]]:
    """Distribute files evenly across workers using round-robin."""
    assignments = {i+1: [] for i in range(worker_count)}
    
    for idx, file_path in enumerate(files):
        worker_id = (idx % worker_count) + 1
        assignments[worker_id].append(file_path)
    
    return assignments


def run_local_mode(
    assignments: Dict[int, List[str]],
    config: Dict[str, any],
    passthrough_args: List[str],
    worker_script: str = 'query_worker.py'
):
    """Run workers as local subprocesses."""
    print(f"Starting {len(assignments)} worker(s) in LOCAL mode")
    print(f"Total files to process: {sum(len(files) for files in assignments.values())}")
    
    workers = []
    
    try:
        for worker_id, files in assignments.items():
            if not files:
                print(f"Worker {worker_id}: No files assigned, skipping")
                continue
            
            print(f"Worker {worker_id}: {len(files)} file(s) assigned")
            
            # Build command
            cmd = [
                sys.executable,
                worker_script,
                '--worker-id', str(worker_id),
                '--files', *files,
                '--hosts', config['scylla_hosts'],
                '--port', str(config['scylla_port']),
                '--keyspace', config['scylla_keyspace'],
                '--table', config['scylla_table'],
                '--batch-size', str(config['batch_size']),
                '--queue-size', str(config['queue_size']),
                '--concurrency', str(config['concurrency']),
                '--query-timeout-secs', str(config['query_timeout_secs']),
                '--metrics-interval-secs', str(config['metrics_interval_secs']),
                '--sort-key-column', config['sort_key_column'],
                '--csv-has-header', str(config['csv_has_header']),
                '--consistency', config['scylla_consistency'],
                '--local-dc', config['local_dc'],
            ]
            
            # Add optional auth
            if config.get('scylla_username'):
                cmd.extend(['--username', config['scylla_username']])
            if config.get('scylla_password'):
                cmd.extend(['--password', config['scylla_password']])
            
            # Add passthrough args (e.g., --dry-run)
            if passthrough_args:
                cmd.extend(passthrough_args)
            
            # Start worker subprocess
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            
            workers.append((worker_id, proc))
        
        # Monitor workers
        def handle_signal(signum, frame):
            print(f"\nReceived signal {signum}, terminating workers...")
            for worker_id, proc in workers:
                proc.terminate()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
        
        # Stream output from all workers
        import select
        
        streams = {proc.stdout.fileno(): (worker_id, proc) for worker_id, proc in workers}
        
        while streams:
            readable, _, _ = select.select(streams.keys(), [], [], 1.0)
            
            for fd in readable:
                worker_id, proc = streams[fd]
                line = proc.stdout.readline()
                
                if line:
                    print(f"[W{worker_id}] {line.rstrip()}")
                else:
                    # Process finished
                    proc.wait()
                    print(f"Worker {worker_id} finished with exit code {proc.returncode}")
                    del streams[fd]
        
        print("\nAll workers completed")
    
    except Exception as e:
        print(f"Error in local mode: {e}")
        for worker_id, proc in workers:
            proc.terminate()
        raise


def generate_docker_compose(
    assignments: Dict[int, List[str]],
    config: Dict[str, any],
    output_file: str
):
    """Generate docker-compose.yml for N workers."""
    print(f"Generating docker-compose configuration for {len(assignments)} worker(s)")
    
    # Map host paths to container paths
    csv_dir_host = config['csv_dir']
    csv_dir_container = '/data'
    
    file_assignments_container = {}
    for worker_id, files in assignments.items():
        container_files = [
            file_path.replace(csv_dir_host, csv_dir_container)
            for file_path in files
        ]
        file_assignments_container[worker_id] = container_files
    
    # Build docker-compose structure
    compose = {
        'version': '3.8',
        'networks': {
            'scylla-net': {
                'driver': 'bridge'
            }
        },
        'services': {}
    }
    
    # Create a service for each worker
    for worker_id, files in file_assignments_container.items():
        if not files:
            continue
        
        service_name = f'worker-{worker_id}'
        
        service = {
            'build': {
                'context': '.',
                'dockerfile': 'Dockerfile'
            },
            'image': 'scylla-query-worker:latest',
            'container_name': service_name,
            'env_file': ['.env'],
            'environment': {
                'WORKER_ID': str(worker_id),
                'ASSIGNED_FILES': ','.join(files)
            },
            'command': [
                '--worker-id', str(worker_id),
                '--files', *files
            ],
            'volumes': [
                f'{csv_dir_host}:{csv_dir_container}:ro'
            ],
            'networks': ['scylla-net'],
            'restart': 'no'
        }
        
        compose['services'][service_name] = service
    
    # Write to file
    with open(output_file, 'w') as f:
        yaml.dump(compose, f, default_flow_style=False, sort_keys=False)
    
    print(f"Generated: {output_file}")
    print(f"\nTo run workers:")
    print(f"  docker compose -f {output_file} up --build")
    print(f"\nTo stop workers:")
    print(f"  docker compose -f {output_file} down")


def save_assignments_manifest(assignments: Dict[int, List[str]], output_file: str):
    """Save file assignments as JSON manifest."""
    manifest = {f'worker-{wid}': files for wid, files in assignments.items()}
    
    with open(output_file, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    print(f"Assignments manifest saved to: {output_file}")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Coordinator: distribute CSV files across query workers'
    )
    
    # Mode
    parser.add_argument(
        '--mode',
        choices=['local', 'docker'],
        required=True,
        help='Execution mode: local subprocesses or docker containers'
    )
    
    # File discovery
    parser.add_argument(
        '--csv-dir',
        default=os.getenv('CSV_DIR', './data'),
        help='Directory containing CSV files'
    )
    parser.add_argument(
        '--file-glob',
        default=os.getenv('FILE_GLOB', '*.csv'),
        help='Glob pattern for CSV files'
    )
    
    # Worker configuration
    parser.add_argument(
        '--worker-count',
        type=int,
        default=int(os.getenv('WORKER_COUNT', '3')),
        help='Number of workers to spawn'
    )
    parser.add_argument(
        '--worker-script',
        default=os.getenv('WORKER_SCRIPT', 'query_worker.py'),
        help='Worker script to use (default: query_worker.py)'
    )
    
    # Output files
    parser.add_argument(
        '--output',
        default='docker-compose.generated.yml',
        help='Output file for docker-compose (docker mode only)'
    )
    parser.add_argument(
        '--assignments',
        default='',
        help='Optional: save assignments manifest to JSON file'
    )
    
    # Worker passthrough (everything after --)
    parser.add_argument(
        'passthrough',
        nargs=argparse.REMAINDER,
        help='Arguments to pass through to workers (e.g., -- --dry-run)'
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Resolve CSV directory to absolute path
    csv_dir = os.path.abspath(args.csv_dir)
    
    if not os.path.isdir(csv_dir):
        print(f"Error: CSV directory not found: {csv_dir}")
        sys.exit(1)
    
    print(f"CSV Directory: {csv_dir}")
    print(f"File Pattern: {args.file_glob}")
    
    # Discover files
    files = discover_files(csv_dir, args.file_glob)
    
    if not files:
        print(f"Error: No files found matching pattern '{args.file_glob}' in {csv_dir}")
        sys.exit(1)
    
    print(f"Discovered {len(files)} file(s)")
    
    # Distribute files across workers
    assignments = distribute_files(files, args.worker_count)
    
    # Show assignments
    print(f"\nFile Distribution:")
    for worker_id, worker_files in assignments.items():
        print(f"  Worker {worker_id}: {len(worker_files)} file(s)")
    
    # Save assignments manifest if requested
    if args.assignments:
        save_assignments_manifest(assignments, args.assignments)
    
    # Load config from environment
    config = {
        'csv_dir': csv_dir,
        'batch_size': int(os.getenv('BATCH_SIZE', '100')),
        'queue_size': int(os.getenv('QUEUE_SIZE', '1000')),
        'concurrency': int(os.getenv('CONCURRENCY', '50')),
        'query_timeout_secs': int(os.getenv('QUERY_TIMEOUT_SECS', '5')),
        'metrics_interval_secs': int(os.getenv('METRICS_INTERVAL_SECS', '5')),
        'sort_key_column': os.getenv('SORT_KEY_COLUMN', 'sort_key'),
        'csv_has_header': os.getenv('CSV_HAS_HEADER', 'true').lower() == 'true',
        'scylla_hosts': os.getenv('SCYLLA_HOSTS', '127.0.0.1'),
        'scylla_port': int(os.getenv('SCYLLA_PORT', '9042')),
        'scylla_keyspace': os.getenv('SCYLLA_KEYSPACE', 'content_db'),
        'scylla_table': os.getenv('SCYLLA_TABLE', 'content_data'),
        'scylla_username': os.getenv('SCYLLA_USERNAME', ''),
        'scylla_password': os.getenv('SCYLLA_PASSWORD', ''),
        'scylla_consistency': os.getenv('SCYLLA_CONSISTENCY', 'LOCAL_ONE'),
        'local_dc': os.getenv('LOCAL_DC', 'aws-us-east-1'),
    }
    
    # Handle passthrough args (strip leading '--' if present)
    passthrough_args = args.passthrough
    if passthrough_args and passthrough_args[0] == '--':
        passthrough_args = passthrough_args[1:]
    
    # Execute based on mode
    if args.mode == 'local':
        run_local_mode(assignments, config, passthrough_args, args.worker_script)
    
    elif args.mode == 'docker':
        generate_docker_compose(assignments, config, args.output)


if __name__ == '__main__':
    main()
