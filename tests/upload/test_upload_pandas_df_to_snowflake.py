import pytest
from snowflake_utilities.upload import upload_pandas_df_to_snowflake
import pandas as pd


@pytest.fixture
def df_iris():
    """example iris dataframe"""
    return pd.read_csv("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv")


@pytest.fixture
def dict_iris():
    """example correct resource type"""
    return pd.read_csv("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv").head(5).to_dict()


class TestUploadPandasDfToSnowflake:
    """Testing class to test the upload_pandas_df_to_snowflake function."""
    def test_upload_w_dict_raises_error(self, dict_iris):
        """Check if the function throws an error if the input is not correct."""
        with pytest.raises(AttributeError):
            _ = upload_pandas_df_to_snowflake(
                data=dict_iris,
                database_name="testdb",
                schema_name="testschema",
                table_name="iris",
            )
    def test_upload_w_df(self, df_iris):
        """Check if the function doesn't throw an error if the input is allowed."""
        upload_pandas_df_to_snowflake(
            data=df_iris,
            database_name="testdb",
            schema_name="testschema",
            table_name="iris",
        )