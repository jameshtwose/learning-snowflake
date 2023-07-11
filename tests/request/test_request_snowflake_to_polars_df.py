import pytest
import polars as pl
import snowflake.connector
from snowflake_utilities.request import request_snowflake_to_polars_df

@pytest.fixture
def df_iris():
    """Fixture to return a polars DataFrame."""
    return pl.read_csv("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv")

class TestRequestSnowflakeToPolarsDf:
    """Testing class to test the request_snowflake_to_polars_df function."""
    def test_request_table_not_exists(self):
        """Check if the function throws an error if the input is not correct."""
        with pytest.raises(snowflake.connector.errors.ProgrammingError):
            _ = request_snowflake_to_polars_df(
                database_name="testdb",
                schema_name="testschema",
                table_name="oooops",
            )
        
    def test_request_polars_df_row_amount(self):
        """Check if the function doesn't throw an error if the input is allowed."""
        df = request_snowflake_to_polars_df(
            database_name="testdb",
            schema_name="testschema",
            table_name="iris",
            row_amount=10,
        )
        assert df.shape[0] == 10
        
    def test_request_polars_df_column_list(self):
        """Check if the function doesn't throw an error if the input is allowed."""
        df = request_snowflake_to_polars_df(
            database_name="testdb",
            schema_name="testschema",
            table_name="iris",
            column_list=["sepal_length", "sepal_width", "species"],
        )
        assert df.shape[1] == 3
        assert df.columns == ["sepal_length", "sepal_width", "species"]