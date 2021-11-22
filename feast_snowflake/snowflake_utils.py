import os
import random
import string
from logging import getLogger
from tempfile import TemporaryDirectory
from typing import Optional

import pandas as pd
import snowflake.connector
from snowflake.connector import ProgrammingError, SnowflakeConnection

getLogger("snowflake.connector.cursor").disabled = True
getLogger("snowflake.connector.connection").disabled = True
getLogger("snowflake.connector.network").disabled = True
logger = getLogger(__name__)


def create_new_snowflake_table(
    conn: SnowflakeConnection,
    df: pd.DataFrame,
    table_name: str,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    compression: str = "gzip",
    on_error: str = "abort_statement",
    parallel: int = 4,
    quote_identifiers: bool = True,
    alt_table_type: str = "permanent",
) -> None:
    cur = conn.cursor()

    result = cur.execute('SELECT * FROM "INFORMATION_SCHEMA"."TABLES"')
    table_names = result.fetch_pandas_all()
    if table_name not in list(
        table_names[table_names["TABLE_SCHEMA"] == conn._schema]["TABLE_NAME"]
    ):

        if database is not None and schema is None:
            raise ProgrammingError(
                "Schema has to be provided to create_new_snowflake_table when a database is provided"
            )

        compression_map = {"gzip": "auto", "snappy": "snappy"}
        if compression not in compression_map.keys():
            raise ProgrammingError(
                "Invalid compression '{}', only acceptable values are: {}".format(
                    compression, compression_map.keys()
                )
            )

        if quote_identifiers:
            location = (
                (('"' + database + '".') if database else "")
                + (('"' + schema + '".') if schema else "")
                + ('"' + table_name + '"')
            )
        else:
            location = (
                (database + "." if database else "")
                + (schema + "." if schema else "")
                + (table_name)
            )

        stage_name = None  # Forward declaration
        while True:
            try:
                stage_name = "".join(
                    random.choice(string.ascii_lowercase) for _ in range(5)
                )
                create_stage_sql = (
                    "create temporary stage /* Python:snowflake.connector.pandas_tools.create_new_snowflake_table() */ "
                    '"{stage_name}"'
                ).format(stage_name=stage_name)
                logger.debug("creating stage with '{}'".format(create_stage_sql))
                cur.execute(create_stage_sql, _is_internal=True).fetchall()
                break
            except ProgrammingError as pe:
                if pe.msg.endswith("already exists."):
                    continue
                raise

        with TemporaryDirectory() as tmp_folder:

            chunk_path = os.path.join(tmp_folder, "file{}.txt".format(0))
            # Dump chunk into parquet file
            df.head().to_parquet(
                chunk_path,
                compression=compression,
                use_deprecated_int96_timestamps=True,
            )
            # Upload parquet file
            upload_sql = (
                "PUT /* Python:snowflake.connector.pandas_tools.create_new_snowflake_table() */ "
                "'file://{path}' @\"{stage_name}\" PARALLEL={parallel}"
            ).format(
                path=chunk_path.replace("\\", "\\\\").replace("'", "\\'"),
                stage_name=stage_name,
                parallel=parallel,
            )
            logger.debug("uploading files with '{}'".format(upload_sql))
            cur.execute(upload_sql, _is_internal=True)
            # Remove chunk file
            os.remove(chunk_path)

        alt_table_types = ["permanent", "transient", "temporary"]
        if alt_table_type.lower() not in alt_table_types:
            raise ProgrammingError(
                "Invalid table type '{}', only acceptable values are: {}".format(
                    alt_table_type, alt_table_types
                )
            )

        table_type_name = ""
        if alt_table_type.lower() in ["transient", "temporary"]:
            table_type_name = alt_table_type.upper()

        # If the table doesnt exists, we need to create it
        file_format_name = "".join(
            random.choice(string.ascii_lowercase) for _ in range(5)
        )

        ff_sql = f"""
        CREATE TEMPORARY FILE FORMAT "{file_format_name}" /* Python:snowflake.connector.pandas_tools.create_new_snowflake_table() */
            TYPE = PARQUET;
        """

        logger.debug("creating parquet file format with '{}'".format(ff_sql))
        cur.execute(ff_sql, _is_internal=True)

        table_sql = f"""
        CREATE {table_type_name} TABLE {location} USING TEMPLATE /* Python:snowflake.connector.pandas_tools.create_new_snowflake_table() */
            (
            SELECT
                ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM
                TABLE(
                    INFER_SCHEMA(
                      LOCATION=>'@"{stage_name}"',
                      FILE_FORMAT=>'"{file_format_name}"'
                    )
                )
            );
        """

        logger.debug(
            "creating new table if one doesnt exist with '{}'".format(table_sql)
        )
        cur.execute(table_sql, _is_internal=True)

        cur.close()
    else:
        raise Exception("Table already exists")
    return None


def get_snowflake_conn(config) -> SnowflakeConnection:

    conn = snowflake.connector.connect(
        account=config.deployment,
        user=config.user,
        password=config.password,
        role=f'"{config.role}"',
        warehouse=f'"{config.warehouse}"',
        database=f'"{config.database}"',
        schema="PUBLIC",
        application="feast",
    )

    return conn
