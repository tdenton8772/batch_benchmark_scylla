#!/bin/bash
# Parallel loading of CSV files into ScyllaDB

cd /data/setup2

# Connection details
HOSTS="node-0.aws-us-east-1.1a3bddd87e6f378a3c21.clusters.scylla.cloud"
PORT=9042
USERNAME="scylla"
PASSWORD="0kXMtmPL3pD8bYs"
KEYSPACE="content_db"

# First: Create keyspace and table (only once)
echo "Creating keyspace and table..."
python3 setup_scylla.py \
  --hosts $HOSTS \
  --port $PORT \
  --username $USERNAME \
  --password $PASSWORD \
  --keyspace $KEYSPACE \
  --csv-pattern '../loadable_data/worker01_*.csv'

# Then: Load remaining files in parallel (split by worker prefix)
echo "Loading CSV files in parallel (15 processes)..."
python3 setup_scylla.py --hosts $HOSTS --port $PORT --username $USERNAME --password $PASSWORD --keyspace $KEYSPACE --csv-pattern '../loadable_data/worker02_*.csv' --skip-create &
python3 setup_scylla.py --hosts $HOSTS --port $PORT --username $USERNAME --password $PASSWORD --keyspace $KEYSPACE --csv-pattern '../loadable_data/worker03_*.csv' --skip-create &
python3 setup_scylla.py --hosts $HOSTS --port $PORT --username $USERNAME --password $PASSWORD --keyspace $KEYSPACE --csv-pattern '../loadable_data/worker04_*.csv' --skip-create &
python3 setup_scylla.py --hosts $HOSTS --port $PORT --username $USERNAME --password $PASSWORD --keyspace $KEYSPACE --csv-pattern '../loadable_data/worker05_*.csv' --skip-create &
python3 setup_scylla.py --hosts $HOSTS --port $PORT --username $USERNAME --password $PASSWORD --keyspace $KEYSPACE --csv-pattern '../loadable_data/worker06_*.csv' --skip-create &
python3 setup_scylla.py --hosts $HOSTS --port $PORT --username $USERNAME --password $PASSWORD --keyspace $KEYSPACE --csv-pattern '../loadable_data/worker07_*.csv' --skip-create &
python3 setup_scylla.py --hosts $HOSTS --port $PORT --username $USERNAME --password $PASSWORD --keyspace $KEYSPACE --csv-pattern '../loadable_data/worker08_*.csv' --skip-create &
python3 setup_scylla.py --hosts $HOSTS --port $PORT --username $USERNAME --password $PASSWORD --keyspace $KEYSPACE --csv-pattern '../loadable_data/worker09_*.csv' --skip-create &
python3 setup_scylla.py --hosts $HOSTS --port $PORT --username $USERNAME --password $PASSWORD --keyspace $KEYSPACE --csv-pattern '../loadable_data/worker10_*.csv' --skip-create &
python3 setup_scylla.py --hosts $HOSTS --port $PORT --username $USERNAME --password $PASSWORD --keyspace $KEYSPACE --csv-pattern '../loadable_data/worker11_*.csv' --skip-create &
python3 setup_scylla.py --hosts $HOSTS --port $PORT --username $USERNAME --password $PASSWORD --keyspace $KEYSPACE --csv-pattern '../loadable_data/worker12_*.csv' --skip-create &
python3 setup_scylla.py --hosts $HOSTS --port $PORT --username $USERNAME --password $PASSWORD --keyspace $KEYSPACE --csv-pattern '../loadable_data/worker13_*.csv' --skip-create &
python3 setup_scylla.py --hosts $HOSTS --port $PORT --username $USERNAME --password $PASSWORD --keyspace $KEYSPACE --csv-pattern '../loadable_data/worker14_*.csv' --skip-create &
python3 setup_scylla.py --hosts $HOSTS --port $PORT --username $USERNAME --password $PASSWORD --keyspace $KEYSPACE --csv-pattern '../loadable_data/worker15_*.csv' --skip-create &
python3 setup_scylla.py --hosts $HOSTS --port $PORT --username $USERNAME --password $PASSWORD --keyspace $KEYSPACE --csv-pattern '../loadable_data/worker16_*.csv' --skip-create &

# Wait for all background jobs to complete
wait

echo "All CSV files loaded!"
