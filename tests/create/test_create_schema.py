import pytest
from snowflake_utilities.create import create_schema
from snowflake_utilities.utils import print_resources


@pytest.fixture
def new_schema_name():
    """Fixture to return a schema name."""
    return "test_schema"


class TestCreateSchema:
    """Testing class to test the flatten function."""

    def test_error_is_raised(self, new_schema_name):
        """Check if the function throws an error if there is no conn specified."""
        with pytest.raises(AttributeError):
            create_schema(conn=None, schema_name=new_schema_name)

    def test_no_error_is_raised(self, new_schema_name):
        """Check if the function works as expected."""
        create_schema(database_name="testdb",
                      schema_name=new_schema_name)
        all_resources = print_resources(resource_type="SCHEMAS")
        assert new_schema_name in [x[1].lower() for x in all_resources]
