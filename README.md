# Feast Snowflake Offline Store Support

**The public releases have passed all integration tests, please create an issue if you got any problem.**

## Change Logs
- DONE [v0.1.0] ~~Submit initial working code~~

## Quickstart

#### Install feast-snowflake

- Install stable version

```shell
pip install feast-snowflake
```

#### Create a feature repository

```shell
feast init feature_repo
cd feature_repo
```

#### Edit `feature_store.yaml`

set `offline_store` type to be `feast_snowflake.SnowflakeOfflineStore`

```yaml
project: ...
registry: ...
provider: local
offline_store:
    type: feast_snowflake.SnowflakeOfflineStore
    deployment: SNOWFLAKE_DEPLOYMENT_URL #drop .snowflakecomputing.com
    user: USERNAME
    password: PASSWORD
    role: ROLE_NAME #remember cap sensitive
    warehouse: WAREHOUSE_NAME #remember cap sensitive
    database: DATABASE_NAME #remember cap sensitive
online_store:
    ...
```

#### Create Hive Table

1. Upload `data/driver_stats.parquet` to Snowflake

```shell
hdfs dfs -copyFromLocal ./data/driver_stats.parquet /tmp/
```

2. Create Hive Table

```sql
CREATE TABLE driver_stats (
    event_timestamp   bigint,
    driver_id         bigint,
    conv_rate         float,
    acc_rate          float,
    avg_daily_trips   int,
    created           bigint
)
STORED AS PARQUET;
```

3. Load data into the table

```sql
LOAD DATA INPATH '/tmp/driver_stats.parquet' INTO TABLE driver_stats;
```

#### Edit `example.py`

```python
# This is an example feature definition file

from google.protobuf.duration_pb2 import Duration

from feast import Entity, Feature, FeatureView, ValueType
from feast_hive import HiveSource

# Read data from Hive table
# Here we use a Query to reuse the original parquet data,
# but you can replace to your own Table or Query.
driver_hourly_stats = HiveSource(
    # table='driver_stats',
    query = """
    SELECT Timestamp(cast(event_timestamp / 1000000 as bigint)) AS event_timestamp,
           driver_id, conv_rate, acc_rate, avg_daily_trips,
           Timestamp(cast(created / 1000000 as bigint)) AS created
    FROM driver_stats
    """,
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)

# Define an entity for the driver.
driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id", )

# Define FeatureView
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=Duration(seconds=86400 * 1),
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    online=True,
    batch_source=driver_hourly_stats,
    tags={},
)
```

#### Apply the feature definitions

```shell
feast apply
```

#### Generating training data and so on

The rest are as same as [Feast Quickstart](https://docs.feast.dev/quickstart#generating-training-data)


## Developing and Testing

#### Developing

```shell
git clone https://github.com/baineng/feast-hive.git
cd feast-hive
# creating virtual env ...
pip install -e ".[dev]"

# before commit
make format
make lint
```

#### Testing

```shell
pip install -e ".[test]"
pytest -n 6 --host=localhost --port=10000 --database=default
```
