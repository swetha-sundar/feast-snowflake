# Feast Snowflake Offline Store Support

## Quickstart

#### Install feast-snowflake

```shell
pip install feast-snowflake
```

#### Create a feature repository

```shell
feast init feature_repo
cd feature_repo
```

#### Edit `feature_store.yaml`

set `offline_store` type to be `feast_snowflake.SnowflakeOfflineStore`<br>
set `online_store` type to be `feast_snowflake.SnowflakeOnlineStore`

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
  type: feast_snowflake.SnowflakeOnlineStore
  deployment: SNOWFLAKE_DEPLOYMENT_URL #drop .snowflakecomputing.com
  user: USERNAME
  password: PASSWORD
  role: ROLE_NAME #remember cap sensitive
  warehouse: WAREHOUSE_NAME #remember cap sensitive
  database: DATABASE_NAME #remember cap sensitive

```

#### Upload sample data to Snowflake

```python
from feast_snowflake.snowflake_utils import create_new_snowflake_table, get_snowflake_conn
from snowflake.connector.pandas_tools import write_pandas
from feast import FeatureStore
import pandas as pd

fs = FeatureStore(repo_path=".")

conn = get_snowflake_conn(fs.config.offline_store)

create_new_snowflake_table(conn, pd.read_parquet('data/driver_stats.parquet'), 'DRIVER_STATS')
write_pandas(conn, pd.read_parquet('data/driver_stats.parquet'), 'DRIVER_STATS')
```

#### Replace the current text in `example.py` with the following:

```python
# This is an example feature definition file
from datetime import timedelta
from feast import Entity, Feature, FeatureView, ValueType
from feast_snowflake import SnowflakeSource
import yaml

# Read data from Snowflake table
# Here we use a Table to reuse the original parquet data,
# but you can replace to your own Table or Query.
database = yaml.safe_load(open("feature_store.yaml"))["offline_store"]["database"]

driver_hourly_stats = SnowflakeSource(
    table=f'"{database}"."PUBLIC"."DRIVER_STATS"',
    #query = """ """,
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)

# Define an entity for the driver.
driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id", )

# Define FeatureView
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=timedelta(weeks=4),
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

#### Work with your Offline & Online Snowflake Feature Store

```python
from example import driver, driver_hourly_stats_view
from datetime import datetime, timedelta
import pandas as pd
from feast import FeatureStore

fs = FeatureStore(repo_path=".")

fs.apply([driver, driver_hourly_stats_view])

# Select features
features = ["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate", "driver_hourly_stats:avg_daily_trips"]

# Create an entity dataframe. This is the dataframe that will be enriched with historical features
entity_df = pd.DataFrame(
    {
        "event_timestamp": [
            pd.Timestamp(dt, unit="ms", tz="UTC").round("ms")
            for dt in pd.date_range(
                start=datetime.now() - timedelta(days=3),
                end=datetime.now(),
                periods=3,
            )
        ],
        "driver_id": [1001, 1002, 1003],
    }
)

# Retrieve historical features by joining the entity dataframe to the Snowflake table source
print("Retrieving training data...")
training_df = fs.get_historical_features(
    features=features, entity_df=entity_df
).to_df()
print(training_df)

print("Loading features into the online store...")
fs.materialize_incremental(end_date=datetime.now())

# Retrieve features from the online store
print("Retrieving online features...")
online_features = fs.get_online_features(
    features=features, entity_rows=[{"driver_id": 1001}, {"driver_id": 1002}],
).to_dict()
print(online_features)
```
