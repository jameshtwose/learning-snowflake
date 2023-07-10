# %%
import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv, find_dotenv

_ = load_dotenv(find_dotenv())

# %%
# Gets the version
ctx = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USERNAME"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
)
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()

# %%
# create a new warehouse
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USERNAME"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
)

cs = conn.cursor()

_ = cs.execute("CREATE WAREHOUSE IF NOT EXISTS tiny_warehouse_mg")
# %%
# specify the warehouse to use (if not already set)
# conn.cursor().execute("USE WAREHOUSE tiny_warehouse_mg")
# %%
# create a new database
_ = cs.execute("CREATE DATABASE IF NOT EXISTS testdb")
# %%
# specify the database to use (if not already set)
# conn.cursor().execute("USE DATABASE testdb")
# %%
# show the list of warehouses
cs.execute("SHOW WAREHOUSES")
all_rows = cs.fetchall()
_ = [print(row) for row in all_rows]
# %%
# show the list of databases
cs.execute("SHOW DATABASES")
all_rows = cs.fetchall()
_ = [print(row) for row in all_rows]
# %%
# create a new schema
_ = cs.execute("CREATE SCHEMA IF NOT EXISTS testschema")
# %%
# specify the schema to use (if not already set)
# _ = cs.execute("USE SCHEMA testschema")
# %%
# show the list of schemas
cs.execute("SHOW SCHEMAS")
all_rows = cs.fetchall()
_ = [print(row) for row in all_rows]
# %%
# create a new table
_ = cs.execute(
    """
    CREATE OR REPLACE TABLE testschema.testtable (
        id INT,
        name STRING
    )
    """
)
# %%
# show the list of tables
cs.execute("SHOW TABLES")
all_rows = cs.fetchall()
_ = [print(row) for row in all_rows]
# %%
# insert some data
_ = cs.execute(
    """
    INSERT INTO testschema.testtable(id, name)
    VALUES (1, 'testname1'), (2, 'testname2')
    """
)
# %%
# print out the data in the table
for c in cs.execute("SELECT * FROM testschema.testtable"):
    print(f"row = ({c[0]}, {c[1]})")
# %%
# save the output of a query to a pandas dataframe
df = pd.read_sql_query("SELECT * FROM testschema.testtable", conn)
# %%
df
# %%
conn.close()
# %%
df
# %%
# conn.cursor().execute("USE DATABASE testdb")
# conn.cursor().execute("USE SCHEMA testschema")
# conn.cursor().execute("PUT file://local_csv.csv @%testtable")
# conn.cursor().execute("COPY INTO testtable")
# %%
