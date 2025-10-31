# QuickStart Guide

Get the query framework running in under 5 minutes.

## Prerequisites

- Python 3.11+
- ScyllaDB cluster (or use existing from `.env` files in `data/setup/`)

## Quick Test (Dry Run - No Database)

Test the framework without connecting to ScyllaDB:

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Test with the included test CSV file (5 rows)
python coordinator.py --mode local --csv-dir . --file-glob test_data.csv --worker-count 2 -- --dry-run
```

You should see output like:
```
CSV Directory: /path/to/loadgen
File Pattern: test_data.csv
Discovered 1 file(s)

File Distribution:
  Worker 1: 1 file(s)
  Worker 2: 0 file(s)

Starting 2 worker(s) in LOCAL mode
...
[W1] 2025-10-30T... | worker=1 | thread=reader | INFO | Reader thread started with 1 file(s)
[W1] 2025-10-30T... | worker=1 | thread=query | INFO | DRY RUN mode - no database connection
[W1] 2025-10-30T... | worker=1 | thread=query | INFO | qps=500.0 | ok=5 | timeouts=0 | errors=0 ...
```

## Test with Real ScyllaDB

### Setup Configuration

```bash
# Copy existing .env from setup directory
cp data/setup/.env .env

# Or create your own .env
cat > .env << 'EOF'
CSV_DIR=.
FILE_GLOB=test_data.csv
WORKER_COUNT=1
BATCH_SIZE=10
SCYLLA_HOSTS=your-scylla-host
SCYLLA_PORT=19042
SCYLLA_KEYSPACE=content_db
SCYLLA_TABLE=content_data
SCYLLA_USERNAME=scylla
SCYLLA_PASSWORD=your-password
EOF
```

### Run Query Workers

```bash
python coordinator.py --mode local --worker-count 1
```

## Generate Larger Test Data

Generate CSV files with the existing data generator:

```bash
cd data/load
python gen.py --num-files 5 --rows-per-file 100 --min-size 200000 --max-size 400000
```

This creates 5 CSV files with 100 rows each (200-400KB per row).

Then run workers against the generated data:

```bash
cd ../..
python coordinator.py --mode local --csv-dir data/load --file-glob 'data_*.csv' --worker-count 3
```

## Docker Mode

Generate docker-compose.yml:

```bash
python coordinator.py --mode docker --csv-dir . --file-glob test_data.csv --worker-count 2
```

Run in containers:

```bash
docker compose -f docker-compose.generated.yml up --build
```

## Next Steps

1. **Scale up**: Increase `WORKER_COUNT` and `CONCURRENCY`
2. **Customize queries**: Edit `query_worker.py` line 261
3. **Monitor**: Watch metrics in logs (QPS, timeouts, errors)
4. **Tune**: Adjust `BATCH_SIZE`, `QUEUE_SIZE`, `CONCURRENCY`, `QUERY_TIMEOUT_SECS`

See [README.md](README.md) for full documentation.
