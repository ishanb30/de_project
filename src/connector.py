"""
connector.py - Connects to Snowflake warehouse, database, and schema based
on credentials read from the process environment.

Assumptions:
    1. The required variables are already present in the process environment -
       written there by load_dotenv from .env when running locally, or by the
       workflow env: block from repository secrets when running in CI.
       Presence is validated up front by _check_env_vars in run.py, so this
       module does not re-check them

    2. Credentials are valid at the time of the call - no retry on auth
       failure, no credential rotation handling

    3. Caller is responsible for closing the connection - no pooling, no
       reuse, no automatic cleanup unless caller uses with

    4. Any errors propagate naturally and are handled by the callers
"""

import snowflake.connector
import os

def get_connection() -> snowflake.connector.SnowflakeConnection:
    connection = snowflake.connector.connect(
        account=os.environ.get("ACCOUNT_IDENTIFIER"),
        user=os.environ.get("USERNAME"),
        password=os.environ.get("PASSWORD"),
        warehouse=os.environ.get("DATA_WAREHOUSE"),
        database=os.environ.get("DATABASE"),
        schema=os.environ.get("SCHEMA"),
        autocommit=False
    )

    return connection

if __name__ == "__main__":
    with get_connection() as conn:
        print("Connection successful")

