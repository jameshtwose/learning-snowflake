import pytest
import pandas as pd
import snowflake.connector
from snowflake_utilities.request import request_snowflake_to_spark_df

@pytest.fixture
def df_iris():
    """Fixture to return a spark DataFrame."""
    return pd.read_csv("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv")

class TestRequestSnowflakeToSparkDf:
    """Testing class to test the request_snowflake_to_spark_df function."""
    def test_request_table_not_exists(self):
        """Check if the function throws an error if the input is not correct."""
        with pytest.raises(snowflake.connector.errors.ProgrammingError):
            _ = request_snowflake_to_spark_df(
                database_name="testdb",
                schema_name="testschema",
                table_name="oooops",
            )
        
    def test_request_spark_df_row_amount(self):
        """Check if the function doesn't throw an error if the input is allowed."""
        df = request_snowflake_to_spark_df(
            database_name="testdb",
            schema_name="testschema",
            table_name="iris",
            row_amount=10,
        )
        assert df.count() == 10
        
    def test_request_spark_df_column_list(self):
        """Check if the function doesn't throw an error if the input is allowed."""
        df = request_snowflake_to_spark_df(
            database_name="testdb",
            schema_name="testschema",
            table_name="iris",
            column_list=["sepal_length", "sepal_width", "species"],
        )
        assert len(df.columns) == 3
        assert df.columns == ["sepal_length", "sepal_width", "species"]