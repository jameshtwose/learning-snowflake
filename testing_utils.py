# %%
from snowflake_utils import upload_local_csv_to_snowflake, upload_pandas_df_to_snowflake, get_snowflake_connection
from dotenv import load_dotenv, find_dotenv
import pandas as pd

_ = load_dotenv(find_dotenv())

# %%
df = pd.read_csv("local_csv.csv")
# %%
df.info()
# %%
df
# %%
upload_local_csv_to_snowflake(
    local_csv_path="local_csv.csv",
    database_name="testdb",
    schema_name="testschema",
    table_name="mytable",
)
# %%
new_df = upload_pandas_df_to_snowflake(
    df=df,
    database_name="testdb",
    schema_name="testschema",
    table_name="mytable",
)
# %%
new_df
# %%
df
# %%
df.info()
# %%
new_df.info()
# %%
