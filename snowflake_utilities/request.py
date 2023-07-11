r"""Submodule request.py includes the following functions:

- **request_snowflake_to_pandas_df** - requests a Snowflake table and saves it to a Pandas DataFrame

- **request_snowflake_to_polars_df** - requests a Snowflake table and saves it to a Polars DataFrame

- **request_snowflake_to_list** - requests a Snowflake table and saves it to a list

- **request_snowflake_to_spark_df** - requests a Snowflake table and saves it to a Spark DataFrame

"""

import snowflake.connector
import pandas as pd
import polars as pl
from pyspark.sql import SparkSession
import pyspark
from .utils import get_snowflake_connection


def request_snowflake_to_pandas_df(
    conn: snowflake.connector.connection.SnowflakeConnection = get_snowflake_connection(),
    database_name: str = "testdb",
    schema_name: str = "testschema",
    table_name: str = "testtable",
    row_amount: int = 1000,
    column_list: list = None,
) -> pd.DataFrame:
    """
    Requests a Snowflake table and saves it to a Pandas DataFrame.

    Parameters
    ----------
    conn : snowflake.connector.connection.SnowflakeConnection
        A connection to Snowflake
    database_name : str
        The name of the database to use
    schema_name : str
        The name of the schema to use
    table_name : str
        The name of the table to use
    row_amount : int
        The amount of rows to request
    column_list : list
        The list of columns to request

    Returns
    -------
    df : pd.DataFrame
        The Pandas DataFrame containing the Snowflake table

    """
    cs = conn.cursor()
    if column_list is None:
        _ = cs.execute(
            f"SELECT * FROM {database_name}.{schema_name}.{table_name} LIMIT {row_amount}"
        )
    else:
        _ = cs.execute(
            f"SELECT {','.join(column_list)} FROM {database_name}.{schema_name}.{table_name} LIMIT {row_amount}"
        )
    df = pd.DataFrame(cs.fetchall())
    df.columns = [col[0].lower() for col in cs.description]
    cs.close()
    return df


def request_snowflake_to_polars_df(
    conn: snowflake.connector.connection.SnowflakeConnection = get_snowflake_connection(),
    database_name: str = "testdb",
    schema_name: str = "testschema",
    table_name: str = "testtable",
    row_amount: int = 1000,
    column_list: list = None,
) -> pl.DataFrame:
    """
    Requests a Snowflake table and saves it to a Polars DataFrame.

    Parameters
    ----------
    conn : snowflake.connector.connection.SnowflakeConnection
        A connection to Snowflake
    database_name : str
        The name of the database to use
    schema_name : str
        The name of the schema to use
    table_name : str
        The name of the table to use
    row_amount : int
        The amount of rows to request
    column_list : list
        The list of columns to request

    Returns
    -------
    df : pl.DataFrame
        The Polars DataFrame containing the Snowflake table

    """
    cs = conn.cursor()
    if column_list is None:
        _ = cs.execute(
            f"SELECT * FROM {database_name}.{schema_name}.{table_name} LIMIT {row_amount}"
        )
    else:
        _ = cs.execute(
            f"SELECT {','.join(column_list)} FROM {database_name}.{schema_name}.{table_name} LIMIT {row_amount}"
        )
    df = pl.DataFrame(cs.fetchall())
    df.columns = [col[0].lower() for col in cs.description]
    cs.close()
    return df


def request_snowflake_to_list(
    conn: snowflake.connector.connection.SnowflakeConnection = get_snowflake_connection(),
    database_name: str = "testdb",
    schema_name: str = "testschema",
    table_name: str = "testtable",
    row_amount: int = 1000,
    column_list: list = None,
) -> list:
    """
    Requests a Snowflake table and saves it to a list.

    Parameters
    ----------
    conn : snowflake.connector.connection.SnowflakeConnection
        A connection to Snowflake
    database_name : str
        The name of the database to use
    schema_name : str
        The name of the schema to use
    table_name : str
        The name of the table to use
    row_amount : int
        The amount of rows to request
    column_list : list
        The list of columns to request

    Returns
    -------
    body_list : list
        The list containing the Snowflake table body.
        Each item in the list is a tuple of values corresponding to rows.
    column_list : list
        The list containing the Snowflake table columns

    """
    cs = conn.cursor()
    if column_list is None:
        _ = cs.execute(
            f"SELECT * FROM {database_name}.{schema_name}.{table_name} LIMIT {row_amount}"
        )
    else:
        _ = cs.execute(
            f"SELECT {','.join(column_list)} FROM {database_name}.{schema_name}.{table_name} LIMIT {row_amount}"
        )
    body_list = cs.fetchall()
    column_list = [col[0].lower() for col in cs.description]
    cs.close()
    return body_list, column_list


def request_snowflake_to_spark_df(
    conn: snowflake.connector.connection.SnowflakeConnection = get_snowflake_connection(),
    spark_session: SparkSession = SparkSession.builder.getOrCreate(),
    database_name: str = "testdb",
    schema_name: str = "testschema",
    table_name: str = "testtable",
    row_amount: int = 1000,
    column_list: list = None,
) -> pyspark.sql.DataFrame:
    """
    Requests a Snowflake table and saves it to a Spark DataFrame.

    Parameters
    ----------
    conn : snowflake.connector.connection.SnowflakeConnection
        A connection to Snowflake
    spark_session : pyspark.sql.SparkSession
        A SparkSession
    database_name : str
        The name of the database to use
    schema_name : str
        The name of the schema to use
    table_name : str
        The name of the table to use
    row_amount : int
        The amount of rows to request
    column_list : list
        The list of columns to request

    Returns
    -------
    df : pyspark.sql.DataFrame
        The Spark DataFrame containing the Snowflake table

    """
    cs = conn.cursor()
    if column_list is None:
        _ = cs.execute(
            f"SELECT * FROM {database_name}.{schema_name}.{table_name} LIMIT {row_amount}"
        )
    else:
        _ = cs.execute(
            f"SELECT {','.join(column_list)} FROM {database_name}.{schema_name}.{table_name} LIMIT {row_amount}"
        )
    body_list = cs.fetchall()
    column_list = [col[0].lower() for col in cs.description]
    df = spark_session.createDataFrame(body_list, column_list)
    cs.close()
    return df
