import itertools
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union
import configparser

import pandas as pd
import pytz
from pydantic.schema import Literal
from pydantic import Field

from feast import Entity
from feast.feature_view import FeatureView
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.usage import log_exceptions_and_usage

from feast.infra.key_encoding_utils import serialize_entity_key
from feast_snowflake.snowflake_utils import (
    create_new_snowflake_table,
    get_snowflake_conn,
    write_pandas_binary
)
from binascii import hexlify

from pathlib import Path
import os

class SnowflakeOnlineStoreConfig(FeastConfigBaseModel):
    """ Online store config for Snowflake """

    type: Literal["feast_snowflake.SnowflakeOnlineStore"] = "feast_snowflake.SnowflakeOnlineStore"
    """ Offline store type selector"""

    config_path: Optional[str] = Path(os.environ['HOME']) / '.snowsql/config'
    """ Snowflake config path -- absolute path required (Cant use ~)"""

    account: Optional[str] = None
    """ Snowflake deployment identifier -- drop .snowflakecomputing.com"""

    user: Optional[str] = None
    """ Snowflake user name """

    password: Optional[str] = None
    """ Snowflake password """

    role: Optional[str] = None
    """ Snowflake role name"""

    warehouse: Optional[str] = None
    """ Snowflake warehouse name """

    database: Optional[str] = None
    """ Snowflake database name """

    schema_: Optional[str] = Field("PUBLIC", alias="schema")
    """ Snowflake schema name """


class SnowflakeOnlineStore(OnlineStore):
    """
    OnlineStore is an object used for all interaction between Feast and the service used for offline storage of
    features.
    """

    @log_exceptions_and_usage(online_store="snowflake")
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        assert isinstance(config.online_store, SnowflakeOnlineStoreConfig)

        snowflake_conn = get_snowflake_conn(config.online_store)

        dfs = [None] * len(data)
        for i, (entity_key, values, timestamp, created_ts) in enumerate(data):

            df = pd.DataFrame(
                columns=[
                    "entity_key",
                    "feature_name",
                    "value",
                    "event_ts",
                    "created_ts",
                ],
                index=range(0, len(values)),
            )

            timestamp = _to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = _to_naive_utc(created_ts)

            for j, (feature_name, val) in enumerate(values.items()):
                df.loc[j, "entity_key"] = serialize_entity_key(entity_key)
                df.loc[j, "feature_name"] = feature_name
                df.loc[j, "value"] = val.SerializeToString()
                df.loc[j, "event_ts"] = timestamp
                df.loc[j, "created_ts"] = created_ts

            dfs[i] = df
            if progress:
                progress(1)

        if dfs:
            agg_df = pd.concat(dfs)

            with snowflake_conn as conn:

                write_pandas_binary(conn, agg_df, f"{config.project}_{table.name}")

                query2 = f"""
                    INSERT OVERWRITE INTO "{config.online_store.database}"."{config.online_store.schema_}"."{config.project}_{table.name}"
                        SELECT
                            "entity_key",
                            "feature_name",
                            "value",
                            "event_ts",
                            "created_ts"
                        FROM
                          (SELECT
                              *,
                              ROW_NUMBER() OVER(PARTITION BY "entity_key","feature_name" ORDER BY "event_ts" DESC, "created_ts" DESC) AS "_feast_row"
                          FROM
                              "{config.online_store.database}"."{config.online_store.schema_}"."{config.project}_{table.name}")
                        WHERE
                            "_feast_row" = 1;
                """

                conn.cursor().execute(query2)
        return None

    @log_exceptions_and_usage(online_store="snowflake")
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        assert isinstance(config.online_store, SnowflakeOnlineStoreConfig)

        snowflake_conn = get_snowflake_conn(config.online_store)

        result: List[Tuple[Optional[datetime], Optional[Dict[str, Any]]]] = []

        with snowflake_conn as conn:

            rows = list(conn.cursor().execute(
                f"""
                SELECT
                    "entity_key", "feature_name", "value", "event_ts"
                FROM
                    "{config.online_store.database}"."{config.online_store.schema_}"."{config.project}_{table.name}"
                WHERE
                    "entity_key" IN ({','.join([('TO_BINARY('+hexlify(serialize_entity_key(entity_key)).__str__()[1:]+")") for entity_key in entity_keys])})
                ORDER BY
                    "entity_key"
            """,

            ).fetch_pandas_all().to_records(index=False))

        rows = {
            k: list(group) for k, group in itertools.groupby(rows, key=lambda r: r[0])
        }
        for entity_key in entity_keys:
            entity_key_bin = serialize_entity_key(entity_key)
            res = {}
            res_ts = None
            for _, feature_name, val_bin, ts in rows.get(entity_key_bin, []):
                val = ValueProto()
                val.ParseFromString(val_bin)
                res[feature_name] = val
                res_ts = ts

            if not res:
                result.append((None, None))
            else:
                result.append((res_ts, res))
        return result

    @log_exceptions_and_usage(online_store="snowflake")
    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        assert isinstance(config.online_store, SnowflakeOnlineStoreConfig)

        snowflake_conn = get_snowflake_conn(config.online_store)

        with snowflake_conn as conn:

            for table in tables_to_keep:
                conn.cursor().execute(
                    f"""CREATE TABLE IF NOT EXISTS "{config.online_store.database}"."{config.online_store.schema_}"."{config.project}_{table.name}" (
                        "entity_key" BINARY,
                        "feature_name" VARCHAR,
                        "value" BINARY,
                        "event_ts" TIMESTAMP,
                        "created_ts" TIMESTAMP
                        )"""
                )

            for table in tables_to_delete:
                conn.cursor().execute(
                    f'DROP TABLE IF EXISTS "{config.online_store.database}"."{config.online_store.schema_}"."{config.project}_{table.name}"'
                )

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        assert isinstance(config.online_store, SnowflakeOnlineStoreConfig)

        snowflake_conn = get_snowflake_conn(config.online_store)

        with snowflake_conn as conn:

            for table in tables:
                query = f'DROP TABLE IF EXISTS "{config.online_store.database}"."{config.online_store.schema_}"."{config.project}_{table.name}"'
                conn.cursor().execute(query)

def _to_naive_utc(ts: datetime):
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)
