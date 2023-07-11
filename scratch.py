# %%
from snowflake_utilities.utils import print_resources
from dotenv import load_dotenv, find_dotenv

_ = load_dotenv(find_dotenv())

# %%
all_rows = print_resources(resource_type="SCHEMAS")
# %%
[x[1].lower() for x in all_rows]
# %%
