import os
import pandas as pd
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


def get_snowflake_connection():
    """
    Returns a connection to Snowflake. Assuming the following environment variables are set:
    SNOWFLAKE_USERNAME - the username
    SNOWFLAKE_PASSWORD - the password
    SNOWFLAKE_ACCOUNT - the account name (the beginning of the URL when you log in to Snowflake)

    Parameters
    ----------
    None : None

    Returns
    -------
    conn : snowflake.connector.connection.SnowflakeConnection
        A connection to Snowflake

    """

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USERNAME"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
    )
    return conn


def create_warehouse(
    conn: snowflake.connector.connection.SnowflakeConnection = get_snowflake_connection(),
    warehouse_name: str = "tiny_warehouse_mg",
):
    """
    Creates a warehouse in Snowflake if it does not already exist.

    Parameters
    ----------
    conn : snowflake.connector.connection.SnowflakeConnection
        A connection to Snowflake
    warehouse_name : str
        The name of the warehouse to create

    Returns
    -------
    None : None

    """
    cs = conn.cursor()
    _ = cs.execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}")
    cs.close()


def create_database(
    conn: snowflake.connector.connection.SnowflakeConnection = get_snowflake_connection(),
    database_name: str = "mydb",
):
    """
    Creates a database in Snowflake if it does not already exist.

    Parameters
    ----------
    conn : snowflake.connector.connection.SnowflakeConnection
        A connection to Snowflake
    database_name : str
        The name of the database to create

    Returns
    -------
    None : None

    """
    cs = conn.cursor()
    _ = cs.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    cs.close()


def create_schema(
    conn: snowflake.connector.connection.SnowflakeConnection = get_snowflake_connection(),
    schema_name: str = "myschema",
):
    """
    Creates a schema in Snowflake if it does not already exist.

    Parameters
    ----------
    conn : snowflake.connector.connection.SnowflakeConnection
        A connection to Snowflake
    schema_name : str
        The name of the schema to create

    Returns
    -------
    None : None

    """
    cs = conn.cursor()
    _ = cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    cs.close()


def print_resources(
    conn: snowflake.connector.connection.SnowflakeConnection = get_snowflake_connection(),
    resource_type: str = "WAREHOUSES",
):
    """
    Prints the list of resources of a given type in Snowflake.

    Parameters
    ----------
    conn : snowflake.connector.connection.SnowflakeConnection
        A connection to Snowflake
    resource_type : str
        The type of resource to print (e.g. WAREHOUSES, DATABASES, SCHEMAS, TABLES)

    Returns
    -------
    all_rows : list
        A list of all rows returned by the query

    """
    check_resource_viable(resource_type=resource_type)

    cs = conn.cursor()
    cs.execute(f"SHOW {resource_type}")
    all_rows = cs.fetchall()
    _ = [print(row) for row in all_rows]
    cs.close()
    return all_rows


def check_resource_viable(resource_type: str = "WAREHOUSES"):
    """
    Checks if a resource type is viable (i.e. it is not a table).

    Parameters
    ----------
    resource_type : str
        The type of resource to check

    Returns
    -------
    is_viable : bool
        True if the resource type is viable, False otherwise

    """
    viable_resource_types = [
        "WAREHOUSE",
        "DATABASE",
        "SCHEMA",
        "TABLE",
        "WAREHOUSES",
        "DATABASES",
        "SCHEMAS",
        "TABLES",
    ]
    is_viable = resource_type in viable_resource_types

    if not is_viable:
        raise ValueError(
            f"Resource type {resource_type} is not viable. Please choose from {viable_resource_types}"
        )


def upload_pandas_df_to_snowflake(
    data: pd.DataFrame = pd.DataFrame(),
    warehouse_name: str = "tiny_warehouse_mg",
    database_name: str = "mydb",
    schema_name: str = "myschema",
    table_name: str = "mytable",
    if_exists: str = "replace",
):
    """
    Uploads a Pandas DataFrame to Snowflake.

    Parameters
    ----------
    data : pd.DataFrame
        The Pandas DataFrame to upload
    warehouse_name : str
        The name of the warehouse to use
    database_name : str
        The name of the database to use in Snowflake
    schema_name : str
        The name of the schema to use in Snowflake
    table_name : str
        The name of the table to use in Snowflake
    if_exists : str
        The action to take if the table already exists in Snowflake
        Options are 'fail', 'replace', 'append'

    Returns
    -------
    new_df : pd.DataFrame
        The Pandas DataFrame that was pulled from Snowflake following the upload

    """
    engine = create_engine(
        URL(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USERNAME"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            database=database_name,
            schema=schema_name,
            warehouse=warehouse_name,
        )
    )

    with engine.connect() as conn, conn.begin():
        data.to_sql(table_name, conn, if_exists=if_exists, index=False)
        new_df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    engine.dispose()

    return new_df
