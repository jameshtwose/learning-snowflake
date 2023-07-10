# %%[markdown]
# ## Uploading the iris data set to a snowflake database
# %%
import pandas as pd
from snowflake_utilities.upload import upload_pandas_df_to_snowflake
from dotenv import load_dotenv, find_dotenv
# %%
# load all the environment variables into the current environment
_ = load_dotenv(find_dotenv())
# %%
# read in the iris data set from the seaborn github repo
df = pd.read_csv("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv")
# %%
# show the first 5 rows of the data set
df.head()
# %%
# upload the data set to snowflake using the upload_pandas_df_to_snowflake function
# N.B. the uploaded data set is returned as a Pandas DataFrame
new_df = upload_pandas_df_to_snowflake(
    data=df,
    warehouse_name="tiny_warehouse_mg",
    database_name="testdb",
    schema_name="testschema",
    table_name="iris",
    if_exists="replace",
)
# %%
# check the first 5 rows of the data set that was pulled from snowflake
new_df.head()
# %%
new_df.shape
# %%
