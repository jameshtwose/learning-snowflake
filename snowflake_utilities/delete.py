r"""Submodule delete.py includes the following functions:

- **delete_warehouse** - deletes a warehouse in Snowflake if it does not already exist

- **delete_database** - deletes a database in Snowflake if it does not already exist

- **delete_schema** - deletes a schema in Snowflake if it does not already exist

"""


import snowflake.connector
from .utils import get_snowflake_connection


def delete_warehouse(
    conn: snowflake.connector.connection.SnowflakeConnection = get_snowflake_connection(),
    warehouse_name: str = "tiny_warehouse_mg",
):
    """
    Deletes a warehouse in Snowflake if it does not already exist.

    Parameters
    ----------
    conn : snowflake.connector.connection.SnowflakeConnection
        A connection to Snowflake
    warehouse_name : str
        The name of the warehouse to delete

    Returns
    -------
    None : None

    """
    cs = conn.cursor()
    _ = cs.execute(f"DROP WAREHOUSE IF EXISTS {warehouse_name}")
    cs.close()
    

def delete_database(
    conn: snowflake.connector.connection.SnowflakeConnection = get_snowflake_connection(),
    database_name: str = "mydb",
):
    """
    Deletes a database in Snowflake if it does not already exist.

    Parameters
    ----------
    conn : snowflake.connector.connection.SnowflakeConnection
        A connection to Snowflake
    database_name : str
        The name of the database to delete

    Returns
    -------
    None : None

    """
    cs = conn.cursor()
    _ = cs.execute(f"DROP DATABASE IF EXISTS {database_name}")
    cs.close()
    

def delete_schema(
    conn: snowflake.connector.connection.SnowflakeConnection = get_snowflake_connection(),
    schema_name: str = "myschema",
):
    """
    Deletes a schema in Snowflake if it does not already exist.

    Parameters
    ----------
    conn : snowflake.connector.connection.SnowflakeConnection
        A connection to Snowflake
    schema_name : str
        The name of the schema to delete

    Returns
    -------
    None : None

    """
    cs = conn.cursor()
    _ = cs.execute(f"DROP SCHEMA IF EXISTS {schema_name}")
    cs.close()
    

def delete_table(
    conn: snowflake.connector.connection.SnowflakeConnection = get_snowflake_connection(),
    database_name: str = "mydb",
    schema_name: str = "myschema",
    table_name: str = "mytable",
):
    """
    Deletes a table in Snowflake if it does not already exist.

    Parameters
    ----------
    conn : snowflake.connector.connection.SnowflakeConnection
        A connection to Snowflake
    table_name : str
        The name of the table to delete

    Returns
    -------
    None : None

    """
    cs = conn.cursor()
    _ = cs.execute(f"USE DATABASE {database_name}")
    _ = cs.execute(f"USE SCHEMA {schema_name}")
    _ = cs.execute(f"DROP TABLE IF EXISTS {table_name}")
    cs.close(
)