import pytest
from snowflake_utilities.create import create_database
from snowflake_utilities.utils import print_resources


@pytest.fixture
def new_database_name():
    """Fixture to return a database name."""
    return "test_database"


class TestCreateDatabase:
    """Testing class to test the flatten function."""

    def test_error_is_raised(self, new_database_name):
        """Check if the function throws an error if there is no conn specified."""
        with pytest.raises(AttributeError):
            create_database(conn=None, database_name=new_database_name)

    def test_no_error_is_raised(self, new_database_name):
        """Check if the function works as expected."""
        create_database(database_name=new_database_name)
        all_resources = print_resources(resource_type="DATABASES")
        assert new_database_name in [x[1].lower() for x in all_resources]
