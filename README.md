<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/ff/Snowflake_Logo.svg/2560px-Snowflake_Logo.svg.png" width="200" align="right">

# snowflake-utilities
A package designed to make working with Snowflake easier. It uses the [snowflake connector](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector) for python in order to setup up and maintain databases and tables in snowflake.

Creating the initial version of the snowflake-utilities package. Mainly to test the skeleton of the structure.

#### To create a new version of the package:
1. Update the version number in the `snowflake_utilities/__init__.py` file
2. Create a new release in github with the same version number (`vX.X.X` as the tag)
    - This will trigger the github action to build the package and upload it to pypi (`.github/workflows/python-publish.yml`)
3. The new version will be available in the [snowflake-utilities pypi](https://pypi.org/project/snowflake-utilities/)

#### To install the package:
- `pip install snowflake-utilities`
