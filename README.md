# ScyllaDB Load Testing Framework

A complete end-to-end load testing framework for ScyllaDB with three phases:
1. **Data Generation** - Multi-threaded CSV generation with realistic size distributions
2. **Data Loading** - Parallel data ingestion into ScyllaDB
3. **Query Load Testing** - Multi-worker query execution with streaming CSV reads

## Architecture

### Overview

The framework consists of two main components:

1. **Coordinator** (`coordinator.py`): Discovers CSV files and distributes them across N workers
2. **Query Workers** (`query_worker.py`): Stateless workers that read CSV files and query ScyllaDB

### Worker Architecture

Each worker has a two-thread architecture:

```
┌─────────────────────────────────────────────────────┐
│                    Query Worker                      │
├─────────────────────────────────────────────────────┤
│                                                       │
│  ┌──────────────┐          ┌──────────────────────┐ │
│  │Reader Thread │          │   Query Thread       │ │
│  │              │          │                      │ │
│  │ • Read CSV   │          │ • Consume batches    │ │
│  │ • Extract    │  Queue   │ • Execute concurrent │ │
│  │   sort_keys  │ ────────>│   queries            │ │
│  │ • Batch keys │          │ • Track metrics      │ │
│  │              │          │                      │ │
│  └──────────────┘          └──────────────────────┘ │
│                                     │                 │
│                                     ▼                 │
│                              ┌─────────────┐         │
│                              │  ScyllaDB   │         │
│                              └─────────────┘         │
└─────────────────────────────────────────────────────┘
```

**Reader Thread**:
- Reads assigned CSV files sequentially
- Extracts `sort_key` column (UUID)
- Batches keys (configurable batch size)
- Queues batches for query thread (with backpressure)
- Sends sentinel when all files processed

**Query Thread**:
- Consumes batches from queue
- Issues concurrent queries using Cassandra driver's `execute_concurrent_with_args`
- Logs timeouts (no retries)
- Tracks metrics: QPS, success rate, timeouts, errors
- Shuts down gracefully when sentinel received

## Project Structure

```
loadgen/
├── gen.py                     # Phase 1: Multi-threaded data generation
├── setup/                     # Phase 2: Database setup & data loading
│   ├── setup_scylla.py       # Create keyspace/table, load CSVs
│   └── .env                  # ScyllaDB connection config
├── setup2/                    # Alternative setup configuration
│   ├── setup_scylla.py       # Same setup script
│   └── .env                  # Different connection config
├── coordinator.py             # Phase 3: Query orchestration
├── query_worker.py            # Phase 3: Query worker implementation
├── requirements.txt           # Python dependencies
├── Dockerfile                 # Worker container image
├── .dockerignore             # Docker build context exclusions
├── .env.example              # Example configuration
└── README.md                 # This file
```

## Workflow Overview

### Phase 1: Data Generation ✅
Generate realistic CSV test data with production-like size distributions (3-150KB per row).

### Phase 2: Data Loading ✅
Load generated CSV files into ScyllaDB cluster with parallel batch ingestion.

### Phase 3: Query Load Testing ✅
Stream CSV files across multiple workers and execute concurrent queries against ScyllaDB.

## Setup

### Prerequisites

- Python 3.11+
- Docker (for container deployment)
- ScyllaDB cluster (local or cloud)

### Local Development

1. **Clone and navigate to project**:
   ```bash
   cd /path/to/loadgen
   ```

2. **Create virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your ScyllaDB connection details
   ```

### Configuration

#### Environment Variables (`.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `CSV_DIR` | `./data` | Directory containing CSV files |
| `FILE_GLOB` | `*.csv` | Glob pattern to match CSV files |
| `SORT_KEY_COLUMN` | `sort_key` | Name of UUID column in CSV |
| `CSV_HAS_HEADER` | `true` | Whether CSV has header row |
| `WORKER_COUNT` | `3` | Number of workers to spawn |
| `BATCH_SIZE` | `100` | Number of keys per batch |
| `QUEUE_SIZE` | `1000` | Max batches in queue |
| `CONCURRENCY` | `50` | Concurrent queries per batch |
| `QUERY_TIMEOUT_SECS` | `5` | Query timeout in seconds |
| `METRICS_INTERVAL_SECS` | `5` | Metrics logging interval |
| `SCYLLA_HOSTS` | `127.0.0.1` | Comma-separated host list |
| `SCYLLA_PORT` | `9042` | CQL port |
| `SCYLLA_KEYSPACE` | `content_db` | Keyspace name |
| `SCYLLA_TABLE` | `content_data` | Table name |
| `SCYLLA_USERNAME` | (empty) | Username for auth |
| `SCYLLA_PASSWORD` | (empty) | Password for auth |
| `SCYLLA_CONSISTENCY` | `LOCAL_QUORUM` | Consistency level |
| `REGION` | `local` | Region tag (for multi-region) |

## Usage

### Phase 1: Generate Test Data

Generate CSV files with realistic production-like size distribution:

```bash
# Generate 36 files with 50K rows each (1.8M total rows, ~14-15GB)
python3 gen.py --output-dir loadable_data --num-files 36 --rows-per-file 50000 --threads 16
```

**Parameters:**
- `--output-dir`: Output directory for CSV files
- `--num-files`: Number of CSV files to generate
- `--rows-per-file`: Rows per file
- `--threads`: Number of parallel threads (default: 4)
- `--min-size`, `--max-size`: Override size distribution (in bytes)
- `--incremental-uuid`: Use sequential UUIDs instead of random

**Output:**
- Files named: `worker01_data_0001.csv`, `worker02_data_0002.csv`, etc.
- Size distribution: Mean ~8KB/row (3.7KB - 151KB range)
- Peak at 4.2KB (22% of rows)
- Progress shown in real-time with throughput stats

**Example Output:**
```
Generating 36 files with 50000 rows each
Output directory: loadable_data
Using 16 threads

✓ File   1/36: loadable_data/worker01_data_0001.csv (50000 rows, keys 1-50000, size 3.6-25.4KB avg=8.1KB)
✓ File   2/36: loadable_data/worker02_data_0002.csv (50000 rows, keys 50001-100000, size 3.6-26.1KB avg=8.0KB)
...

================================================================================
Completed in 45.2s
Total files: 36
Total rows: 1,800,000
Throughput: 39,823 rows/sec
================================================================================
```

**Long-Running Generation (with screen):**
```bash
# Start screen session
screen -S datagen

# Run generation
python3 gen.py --output-dir loadable_data --num-files 36 --rows-per-file 50000 --threads 16

# Detach: Press Ctrl+A, then D
# Reconnect later: screen -r datagen
```

### Phase 2: Load Data into ScyllaDB

Load generated CSV files into ScyllaDB:

```bash
cd setup
python3 setup_scylla.py --csv-pattern '../loadable_data/worker*.csv'
```

**Alternative configuration:**
```bash
cd setup2
python3 setup_scylla.py --csv-pattern '../loadable_data/worker*.csv'
```

**Parameters:**
- `--hosts`: ScyllaDB host(s) (comma-separated)
- `--port`: CQL port (default: 9042)
- `--keyspace`: Keyspace name (default: content_db)
- `--username`, `--password`: Authentication credentials
- `--csv-pattern`: Glob pattern for CSV files (supports `**` recursive)
- `--batch-size`: Batch size for loading (default: 50)
- `--skip-create`: Skip keyspace/table creation (only load data)
- `--replication-factor`: Replication factor (default: 3)

**Example:**
```bash
# With authentication
python3 setup_scylla.py \
  --hosts node1.cluster.cloud,node2.cluster.cloud \
  --port 9042 \
  --keyspace content_db \
  --username scylla \
  --password mypassword \
  --csv-pattern '../loadable_data/*.csv' \
  --batch-size 100
```

### Phase 3: Run Query Load Tests

#### Local Mode (Subprocesses)

Run workers as local subprocesses (useful for development):

```bash
python coordinator.py --mode local --csv-dir ./data/load --worker-count 3
```

**With dry-run** (no database queries):
```bash
python coordinator.py --mode local --csv-dir ./data/load --worker-count 2 -- --dry-run
```

**Save assignments manifest**:
```bash
python coordinator.py --mode local --csv-dir ./data/load --worker-count 3 --assignments assignments.json
```

#### Docker Mode (Containers)

Generate docker-compose.yml:

```bash
python coordinator.py --mode docker --csv-dir ./data/load --worker-count 5 --output docker-compose.generated.yml
```

Run workers in containers:

```bash
docker compose -f docker-compose.generated.yml up --build
```

Stop workers:

```bash
docker compose -f docker-compose.generated.yml down
```

### Monitoring

Workers log metrics every `METRICS_INTERVAL_SECS` seconds:

```
2025-10-30T12:34:56Z | worker=3 | thread=query | INFO | qps=480.2 | ok=9600 | timeouts=3 | errors=1 | submitted=9604 | rows_read=9800 | files=2 | queue_depth=50
```

**Metrics**:
- `qps`: Queries per second (rolling window)
- `ok`: Successful queries
- `timeouts`: Query timeouts (logged, not retried)
- `errors`: Other errors
- `submitted`: Total queries submitted
- `rows_read`: Total rows read from CSV
- `files`: Files processed
- `queue_depth`: Current queue size

### Graceful Shutdown

Workers handle `SIGINT` (Ctrl+C) and `SIGTERM` gracefully:
1. Reader thread stops processing new files
2. Query thread drains remaining batches
3. Database connections closed cleanly
4. Final metrics logged

## Query Logic

### Current Implementation

Workers execute a simple SELECT by primary key:

```sql
SELECT * FROM {keyspace}.{table} WHERE sort_key = ?
```

The Cassandra driver's `execute_concurrent_with_args` is used for concurrent execution within each batch, NOT CQL `BATCH` statements.

### Customization

To customize the query:
1. Edit `query_worker.py`, line 261
2. Modify the CQL statement as needed
3. Adjust the arguments list preparation (line 299) accordingly

## File Distribution

The coordinator distributes CSV files evenly across workers using round-robin:

**Example**: 10 files, 3 workers
- Worker 1: files 1, 4, 7, 10
- Worker 2: files 2, 5, 8
- Worker 3: files 3, 6, 9

## Future Enhancements

### S3 Integration

Replace local file reading with S3 streaming:

1. Add `boto3` to requirements.txt
2. Create `S3FileSource` class implementing streaming reads
3. Update coordinator to list S3 objects
4. Workers download and stream S3 objects

### Multi-Region Deployment

Deploy workers across 3 regions (e.g., us-east-1, us-west-2, eu-west-1):

1. Build and push Docker image to registry (ECR, Docker Hub)
2. Deploy workers in each region using:
   - ECS/Fargate
   - Kubernetes
   - VM-based deployments
3. Use region-specific CSV file lists or S3 prefixes
4. Tag each worker with `REGION` environment variable
5. Workers query nearest ScyllaDB cluster nodes

### Observability

- Export metrics to Prometheus
- Structured JSON logging
- Distributed tracing (OpenTelemetry)
- Health check endpoints

## Troubleshooting

### No files found

```
Error: No files found matching pattern '*.csv' in ./data
```

**Solution**: Generate CSV files or check `CSV_DIR` and `FILE_GLOB` configuration.

### Connection refused

```
Error: NoHostAvailable
```

**Solution**: Verify `SCYLLA_HOSTS` and `SCYLLA_PORT` are correct. Check network connectivity.

### Query timeouts

```
WARNING | Query timeout: OperationTimedOut
```

**Solution**: 
- Increase `QUERY_TIMEOUT_SECS`
- Reduce `CONCURRENCY`
- Check ScyllaDB cluster health and load

### Queue full / Worker hanging

**Solution**:
- Increase `QUEUE_SIZE`
- Increase `CONCURRENCY` to drain queue faster
- Check if query thread is stuck (database connectivity issues)

## Caveats

- **Batch Queries**: Uses Cassandra driver's `execute_concurrent_with_args`, NOT CQL `BATCH` statements
- **Primary Key**: Ensure `sort_key` column name matches your schema
- **UUID Format**: CSV must contain valid UUID strings
- **Memory**: Large `QUEUE_SIZE` and `BATCH_SIZE` consume more memory
- **File Handles**: Each worker opens files sequentially (not concurrently)

## Contributing

When extending this framework:
1. Maintain the stateless worker design
2. Keep configuration via environment variables
3. Log errors, don't swallow them
4. Use structured logging for observability
5. Test with dry-run mode first

## License

(Add your license here)
