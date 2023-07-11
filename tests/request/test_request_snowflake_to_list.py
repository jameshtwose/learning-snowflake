import pytest
import pandas as pd
import snowflake.connector
from snowflake_utilities.request import request_snowflake_to_list

@pytest.fixture
def df_iris():
    """Fixture to return a Pandas DataFrame."""
    return pd.read_csv("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv")

class TestRequestSnowflakeToList:
    """Testing class to test the request_snowflake_to_list function."""
    def test_request_table_not_exists(self):
        """Check if the function throws an error if the input is not correct."""
        with pytest.raises(snowflake.connector.errors.ProgrammingError):
            _ = request_snowflake_to_list(
                database_name="testdb",
                schema_name="testschema",
                table_name="oooops",
            )
    def test_request_list(self, df_iris):
        """Check if the function doesn't throw an error if the input is allowed."""
        body_list, columns_list = request_snowflake_to_list(
            database_name="testdb",
            schema_name="testschema",
            table_name="iris",
        )
        assert len(body_list) == df_iris.shape[0]
        assert len(columns_list) == df_iris.shape[1]
        assert columns_list == df_iris.columns.tolist()
        
    def test_request_list_row_amount(self, df_iris):
        """Check if the function doesn't throw an error if the input is allowed."""
        body_list, columns_list = request_snowflake_to_list(
            database_name="testdb",
            schema_name="testschema",
            table_name="iris",
            row_amount=10,
        )
        assert len(body_list) == 10
        assert len(columns_list) == df_iris.shape[1]
        assert columns_list == df_iris.columns.tolist()
        
    def test_request_list_column_list(self, df_iris):
        """Check if the function doesn't throw an error if the input is allowed."""
        body_list, columns_list = request_snowflake_to_list(
            database_name="testdb",
            schema_name="testschema",
            table_name="iris",
            column_list=["sepal_length", "sepal_width", "species"],
        )
        assert len(body_list) == df_iris.shape[0]
        assert len(columns_list) == 3
        assert columns_list == ["sepal_length", "sepal_width", "species"]