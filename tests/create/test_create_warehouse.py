import pytest
from snowflake_utilities.create import create_warehouse
from snowflake_utilities.utils import print_resources


@pytest.fixture
def new_warehouse_name():
    """Fixture to return a warehouse name."""
    return "test_warehouse"


class TestCreateWarehouse:
    """Testing class to test the flatten function."""

    def test_error_is_raised(self, new_warehouse_name):
        """Check if the function throws an error if there is no conn specified."""
        with pytest.raises(AttributeError):
            create_warehouse(conn=None, warehouse_name=new_warehouse_name)

    def test_no_error_is_raised(self, new_warehouse_name):
        """Check if the function works as expected."""
        create_warehouse(warehouse_name=new_warehouse_name)
        all_resources = print_resources(resource_type="WAREHOUSES")
        assert new_warehouse_name in [x[0].lower() for x in all_resources]
