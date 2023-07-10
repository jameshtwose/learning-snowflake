from snowflake_utilities.utils import check_resource_viable
from dotenv import load_dotenv, find_dotenv

_ = load_dotenv(find_dotenv())

check_resource_viable(resource_type="WAREHOUSES")